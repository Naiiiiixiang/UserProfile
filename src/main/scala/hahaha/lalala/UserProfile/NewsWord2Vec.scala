package hahaha.lalala.UserProfile

import hahaha.lalala.UserProfile.conf.Config
import hahaha.lalala.UserProfile.util.SparkHelper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{explode, udf}
import org.slf4j.LoggerFactory

/**
 * @Description 训练文章词向量
 *              输入数据表 dwb_news.article_top_terms
 *              输出数据表 dwb_news.article_top_terms_w2v
 * @Author niyaolanggeyo
 * @Date 2021/8/20 18:12
 * @Version 1.0
 */
object NewsWord2Vec {

  private val log = LoggerFactory.getLogger("news-word2vec")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    System.setProperty("HADOOP_USER_NAME", "ly")

    // 解析命令行参数
    val params = Config.parseConfig(NewsWord2Vec, args)
    log.warn("Job running please wait ... ")
    // 取多少个关键词
    val topK = params.topK
    // init spark session
    val ss = SparkHelper.getSparkSession(params.env, "news-word2vec")

    // 如果是本地测试，限制一下读取数据条数
    var limitData = ""
    if (params.env.equalsIgnoreCase("dev")) {
      limitData = "limit 1000"
    }

    // 读取 dwb_news.article_cn 表的数据, 该表是分词后的数据
    val sourceTermsDataSQL =
      s"""
         |select
         |article_id,
         |terms,
         |top_terms
         |from
         |dwb_news.article_top_terms
         |where
         |terms is not null
         |and
         |top_terms is not null
         |$limitData
      """.stripMargin
    val sourceDF = ss.sql(sourceTermsDataSQL)
    import ss.implicits._

    // 将DF中terms,top_terms转换为数组
    val documentDF = sourceDF.map(elem => {
      val terms = elem.getAs("terms").toString.split(",")
      val top_terms = elem.getAs("top_terms").toString.split(",")
      val article_id = elem.getAs("article_id").toString
      (article_id, terms, top_terms)
    }).toDF("article_id", "terms", "top_terms")

    // word2vec 模型
    val word2VecModel = new Word2Vec()
      .setInputCol("terms") // 将文章分词的数据输入到模型
      .setOutputCol("vector") // 设置输出列名字
      .setVectorSize(64) // 设置词向量的维度，就是想要将词映射到多少维的空间内，大语料集一般选择 128，256
      .setMinCount(5) // 只要超过一定词频的词才会参于训练，才会被参与训练，默认是5
      .setNumPartitions(2) // 语料库数据集训练时的分区数，分区数越多，速度越快，但是精度越低
      .setMaxIter(2) // 迭代次数，一般小于或等于分区数据。迭代次数越多，精度越高，速度越慢
      .setWindowSize(8) // 窗口大小，默认5，还记得文档上我们说过Word2Vec可以学习出词的上下文信息,这个参数就影响了，一个词周围多少个词作为上下文
      .fit(documentDF)

    /**
     * 获取句子的向量表示，源码实现方式是，将一个句子的所有词向量加和求平均。 我们这里暂时不用
     * 这里其实就是把一篇文章表示为向量了，当我们计算文章直接的相似度时，完全可以用这个向量计算
     * 单这里我们不直接使用文章向量，而是取文章每个词的词向量，保存起来，为以后根据关键词计算用户向量做准备
     * word2VecModel.transform(documentDF)
     *
     * 获取每个词的词向量，同时建立一个临时表
     * 得到数据样式如下：
     * +----------+----------------------------------------
     * |word|vector                                      |
     * +----------+------------------------------------------
     * |食物    |[-0.12571518123149872,-0.2230015993118286,.....]  |
     * |时间    |[-0.024214116856455803,-0.1975427269935608,....] |
     */
    val w2v = word2VecModel.getVectors

    //包括word，vector列
    //    w2v.show(10)

    // 将vector转换为array函数
    val toArr: Any => Array[Double] = _.asInstanceOf[DenseVector].toArray
    // 定义udf
    val toArrUdf = udf(toArr)
    w2v.withColumn("vector", toArrUdf('vector)).createTempView("word_vec")


    /**
     * 将原始的top分词表数据中的 top_terms 列的分词数据每个词拆成一行，目的是
     * 和我们计算出来的词向量表，刚才创建的临时表 `word_vec` 做关联，获取到
     * 每个关键词的词向量表示
     */
    documentDF.withColumn("top_term", explode($"top_terms"))
      .createTempView("source_term")

    // 将source_term临时表和word_vec临时表做关联，获取每个关键词词向量
    val termVecSQl =
      """
        |with t1 as (
        |select
        |article_id,top_term
        |from
        |source_term
        |),
        |t2 as (
        |select
        |word,
        |vector
        |from
        |word_vec
        |)
        |select
        |article_id,
        |top_term,
        |vector
        |from
        |t1 left join t2
        |on
        |t1.top_term=t2.word
        |""".stripMargin

    /**
     * // 执行关联之后的结果，类似如下
     * +----------+--------+--------------------------------------------+
     * |article_id|top_term|vector                                      |
     * +----------+--------+--------------------------------------------+
     * |8796535   |食物    |[-0.12571518123149872,-0.2230015993118286]  |
     * |8796535   |时间    |[-0.024214116856455803,-0.1975427269935608] |
     * |8796535   |性激素  |[0.11802277714014053,-0.05457611382007599]  |
     * |8796535   |男孩    |[-0.20423880219459534,0.1379462629556656]   |
     */
    val keyWordsVec = ss.sql(termVecSQl)

    // 将生成的关键词向量数据保存HDFS,数仓dwb层，表为 dwb_news.article_terms_w2v
    keyWordsVec.write.mode(SaveMode.Overwrite).format("ORC").saveAsTable("dwb_news.article_top_terms_w2v")
    ss.stop()
    log.warn("Job finish success !")
  }
}
