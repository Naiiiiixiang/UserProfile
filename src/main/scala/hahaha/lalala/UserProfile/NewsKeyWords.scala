package hahaha.lalala.UserProfile

import hahaha.lalala.UserProfile.conf.Config
import hahaha.lalala.UserProfile.util.SparkHelper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{CountVectorizer, IDF}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.SaveMode
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
 * @Description TF-IDF 提取文章关键词
 *              输入数据表dwb_news.article_terms
 *              输出数据表 dwb_news.article_top_terms
 * @Author niyaolanggeyo
 * @Date 2021/8/20 18:11
 * @Version 1.0
 */
object NewsKeyWords {
  private val log = LoggerFactory.getLogger("news-keywords")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    System.setProperty("HADOOP_USER_NAME", "ly")

    // 解析命令行参数
    val params = Config.parseConfig(NewsKeyWords, args)
    log.warn("Job running please wait ... ")

    // 取多少个关键词
    val topK = params.topK

    // init spark session
    val ss = SparkHelper.getSparkSession(params.env, "news-keywords")
    import ss.implicits._

    // 如果是本地测试，限制一下读取数据条数
    var limitData = ""
    if (params.env.equalsIgnoreCase("dev")) {
      limitData = "limit 1000"
    }

    // 读取 dwb_news.article_terms  表的数据, 该表是分词后的数据
    val sourceTermDataSQL =
      s"""
         |select
         |article_id,
         |content_terms
         |from
         |dwb_news.article_terms
         |where
         |content_terms is not null
         |$limitData
         |""".stripMargin
    val sourceDF = ss.sql(sourceTermDataSQL)

    // 将DF中content_terms转换为数组
    val documentDF = sourceDF.map(elem => {
      val terms = elem.getAs("content_terms").toString.split(",")
      val article_id = elem.getAs("article_id").toString
      (article_id, terms)
    }).toDF("article_id", "terms")

    /**
     * hash 方式获取 TF
     * 这个方法是将词汇表映射到固定的桶内
     * 避免了为每一个词生成一个ID映射桶的个数由setNumFeatures指定
     * 但是此方法是无法获取精确的词频的
     */
    //    val hashingTF = new HashingTF().setInputCol("terms").setOutputCol("rawFeatures").setNumFeatures(512)
    //    val featurizedData = hashingTF.transform(documentDF)

    // CountVectorizer 可以得到精确的TF值，我们采用此方法
    val cvModel = new CountVectorizer()
      .setInputCol("terms") // 输入的列，这一列就是我们分好的词
      .setOutputCol("rawFeatures") // 输出列
      .setVocabSize(512 * 512) // 词汇表大小
      .setMinDF(1) // 一个词至少在几个不同文档中出现才计算在内
      .fit(documentDF)

    /**
     * 执行 CountVectorizer, 之后生成的结果, rawFeatures列的每行值会类似如下
     * (461,[8,93,134,245,270,379,458,459],[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0])
     * 461 表示词汇表大小，就是我们设置的VocabSize值。如果你的词汇表大小没有达到设定值，会按照你词汇表的值
     * 第一个数组表示，每个词在词汇表的索引值
     * 第二个数组表示，每个词出现的词频
     */
    val featurizedData = cvModel.transform(documentDF)

    // IDF 模型，输入是 rawFeatures ,就是 CountVectorizerModel 的输出列
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    // 计算TF-IDF值
    val rescaledData = idfModel.transform(featurizedData)

    val topTermsDF = rescaledData.select("article_id", "terms", "features")
      .map(elem => {
        val article_id = elem.getAs("article_id").toString
        val terms = elem.getAs[Seq[String]]("terms")

        /**
         * features 类似如下
         * (461,[8,93,134,245,270,379,458,459],[1.0116009116784799,1.7047480922384253,1.7047480922384253,1.7047480922384253,1.7047480922384253,1.7047480922384253,1.7047480922384253,1.7047480922384253])
         * 这是一个稀疏向量的标识，第一个值表示向量的维度，第二个数据表示维度的索引，同时是所有在该维度上有值的索引。
         * 第三个数据是每个维度下的值，也就是每个词的TF-IDF值
         */
        val features = elem.getAs[Vector]("features")

        val tfidfValueArray = ArrayBuffer[Double]()
        features.foreachActive((_: Int, value: Double) => {
          tfidfValueArray += value
        })
        // 将TF-IDF和原始词做zip，按照TF-IDF值大小排序, 取 topk 的词
        val topKTerms = terms.zip(tfidfValueArray)
          .toList
          .sortBy(_._2)
          .take(topK)
          .toMap
          .keys
          .toArray
          .mkString(",")
        (article_id, topKTerms, terms.mkString(","))
      }).toDF("article_id", "topKTerms", "terms")

    // topTermsDF.show(false)
    /**
     * 生成的 topk 关键词写入到HDFS，数仓dwb层
     * 表为 dwb_news.article_top_terms
     */
    topTermsDF.write.mode(SaveMode.Overwrite).format("ORC").saveAsTable("dwb_news.article_top_terms")
    ss.stop()
    log.warn("Job finish success !")


  }

}
