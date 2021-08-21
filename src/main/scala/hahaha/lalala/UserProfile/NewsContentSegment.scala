package hahaha.lalala.UserProfile

import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary
import com.hankcs.hanlp.tokenizer.StandardTokenizer
import hahaha.lalala.UserProfile.conf.Config
import hahaha.lalala.UserProfile.util.SparkHelper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
 * @Description 对文章数据进行中文分词
 *              查询数据表 dwb_news.article_cn
 *              生成数据表 dwb_news.article_terms
 * @Author niyaolanggeyo
 * @Date 2021/8/20 18:11
 * @Version 1.0
 */
object NewsContentSegment {
  private val log = LoggerFactory.getLogger("new-content_segment")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    System.setProperty("HADOOP_USER_NAME", "ly")

    // 解析命令行参数
    val params = Config.parseConfig(NewsContentSegment, args)
    log.warn("Job running please wait ... ")

    // init spark session
    val ss = SparkHelper.getSparkSession(params.env, "news-content-segment")
    // 非常重要的导入，会导致无法推断
    import ss.implicits._

    // 如果是本地测试，限制一下读取数据条数
    var limitData = ""
    if (params.env.equalsIgnoreCase("dev")) {
      limitData = "limit 1000"
    }

    // 读取 dwb_news.article_cn 表的数据
    val sourceArticleDataSQL =
      s"""
         |select
         |article_id,
         |content
         |from
         |dwb_news.article_cn
         |$limitData
         |""".stripMargin

    val sourceDF = ss.sql(sourceArticleDataSQL)

    val termDF = sourceDF.mapPartitions(partition => {
      // debug模式，上线不要开启，影响性能
      // HanLP.Config.enableDebug()
      var resTermList = List[(String, String)]()
      for (elem <- partition) {
        val articleId = elem.getAs("article_id").toString
        val content = elem.getAs("content").toString
        val termList = StandardTokenizer.segment(content)

        // 去除停用词
        val termListNoStop = CoreStopWordDictionary.apply(termList)
        val contentTerm = termListNoStop.filter(term => {
          // 只保留名词相关的词，即词性是n开头的词, 同时去掉单个字
          term.nature.startsWith("n") && term.word.length != 1
        }).
          // 只取出词不要词性，去重后，以逗号分隔
          map(term => term.word).distinct.mkString(",")
        val res = (articleId, contentTerm)
        // 去掉空值
        if (contentTerm.nonEmpty) {
          resTermList = res :: resTermList
        }
      }
      resTermList.iterator
    }).toDF("article_id", "content_terms")
    // 将分词后的数据保存到HDFS上，数仓dwb层，表为dwb_news.article_terms， 表中两个字段，分别为article_id 和 content_terms
    termDF.write.mode(SaveMode.Overwrite).format("ORC").saveAsTable("dwb_news.article_terms")
    ss.stop()
    log.warn("Job finish success !")
  }
}
