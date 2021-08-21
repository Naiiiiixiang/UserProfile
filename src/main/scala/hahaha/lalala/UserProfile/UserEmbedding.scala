package hahaha.lalala.UserProfile

import hahaha.lalala.UserProfile.conf.Config
import hahaha.lalala.UserProfile.util.SparkHelper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.udf
import org.slf4j.LoggerFactory

/**
 * @Description 生成用户的Embedding向量, 输入数据
 *              1. ods_news.event表中用户对文章的点击数据
 *                 2. dwb_news.article_top_terms_w2v 文章 topk 关键词向量表数据
 *                 两表关联, 获取到用户的点击过文章的所有关键词向量,
 *                 之后将所有关键词向量加和求平均, 得到用户的向量表示.
 *                 存储到 dws_news.user_content_embedding表中
 * @Author niyaolanggeyo
 * @Date 2021/8/20 18:12
 * @Version 1.0
 */
object UserEmbedding {
  private val log = LoggerFactory.getLogger("user-embedding")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    System.setProperty("HADOOP_USER_NAME", "ly")

    // 解析命令行参数
    val params = Config.parseConfig(UserEmbedding, args)
    log.warn("Job running please wait ... ")
    // 取多少个关键词
    val topK = params.topK
    // init spark session
    val ss = SparkHelper.getSparkSession(params.env, "user-embedding")
    // spark sql 正确读取 hudi 表的配置
    ss.sparkContext.hadoopConfiguration.setClass("mapreduce.input.pathFilter.class",
      classOf[org.apache.hudi.hadoop.HoodieROTablePathFilter],
      classOf[org.apache.hadoop.fs.PathFilter])
    import ss.implicits._

    // 如果是本地测试，限制一下读取数据条数
    var limitData = ""
    if (params.env.equalsIgnoreCase("dev")) {
      limitData = "limit 100"
    }

    // 管理ods_news.event 和dwb_news.article_top_terms_w2v,以文章ID为关联条件，得到用户在每个文章下的词向量表
    val userWordVecSQL =
      s"""
         |with t1 as(
         |  select
         |      distinct_id as uid,
         |      article_id
         |  from
         |      ods_news.event
         |  where
         |      logday >= '${params.startDate}'
         |      and
         |      logday <= '${params.endDate}'
         |      and
         |      event = 'AppClick'
         |      and
         |      element_page = '内容详情页'
         |      and
         |      article_id != ''
         |      and
         |      article_id is not null
         |),
         |t2 as(
         |   select
         |       article_id,
         |       vector
         |   from
         |       dwb_news.article_top_terms_w2v
         |)
         |select
         |    t1.uid,
         |    t1.article_id,
         |    t2.vector
         |from
         |    t1 left join t2
         |    on
         |    t1.article_id = t2.article_id
         |where
         |    t2.vector is not null
         |$limitData
         |""".stripMargin

    val userWordVecDF = ss.sql(userWordVecSQL)
    userWordVecDF.show(3)

    /**
     * 定义函数
     * 将array[double]转换为稠密向量
     * 因为我们之前存到表里的词向量是array[double]类型的
     * 这里转为稠密向量DenseVector
     * 方便计算
     */
    val array2vec = udf((array: Seq[Double]) => {
      Vectors.dense(array.toArray)
    })

    // 以用户ID为维度，将所有该用户下的词向量加和求平均得到用户向量
    val userEmbeddingDF = userWordVecDF
      .withColumn("vector", array2vec($"vector"))
      .groupBy("uid")
      .agg(Summarizer.mean($"vector").alias("user_vector"))

    // 将vector转换为array函数
    val toArr: Any => Array[Double] = _.asInstanceOf[DenseVector].toArray
    // 定义udf
    val toArrUdf = udf(toArr)
    userEmbeddingDF.show(3)

    /**
     * 将生成的关键词向量数据保存HDFS
     * 数仓dwb层
     * 表为dws_news.user_content_embedding
     */
    userEmbeddingDF.withColumn("user_vector", toArrUdf($"user_vector"))
      .write.mode(SaveMode.Overwrite).format("ORC").saveAsTable("dws_news.user_content_embedding")
    ss.stop()
    log.warn("Job finish success !")

  }
}
