package hahaha.lalala.UserProfile

import hahaha.lalala.UserProfile.conf.Config
import hahaha.lalala.UserProfile.util.{SparkHelper, TableUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode
import org.slf4j.LoggerFactory


/**
 * @Description 生成基础特征标签，生成后的标签写入到 ClickHouse
 *              1. 性别 2.年龄 3.电话归属地 4. 邮箱尾号 5. 手机机型
 *                 五个基础特征标签
 * @Author niyaolanggeyo
 * @Date 2021/8/20 17:55
 * @Version 1.0
 */
object LabelGenerator {
  private val log = LoggerFactory.getLogger("label-generator")


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    System.setProperty("HADOOP_USER_NAME", "ly")

    // 解析命令行参数
    val params = Config.parseConfig(LabelGenerator, args)
    log.warn("Job running please wait ... ")

    // init spark session
    val ss = SparkHelper.getSparkSession(params.env, "label-generator")

    /**
     * 如果是hudi的MOR表，需要以下配置，因为默认spark会使用自己的Parquet reader 代替hive 的Hive的SerDe
     * 因为我们的MOR表是parquet+avro,因此要设置如下参数，强制让Spark使用Hive的SerDe
     * ss.sparkContext.getConf.set("spark.sql.hive.convertMetastoreParquet","false")
     * spark sql 正确读取hudi表的配置
     */
    ss.sparkContext.hadoopConfiguration.setClass("mapreduce.input.pathFilter.class",
      classOf[org.apache.hudi.hadoop.HoodieROTablePathFilter],
      classOf[org.apache.hadoop.fs.PathFilter])

    /**
     * 查询生成用户基础标签信息，ods_news.user 和 dwb_news.user_base_info关联， 生成表 dws_news.user_profile_base
     * 生成表结构段信息类似如下
     *
     * +--------+------+---+-----------+------------+---------+
     * |     uid|gender|age|     region|email_suffix|    model|
     * +--------+------+---+-----------+------------+---------+
     * |51810600|    女| 22|15686823255|   yahoo.com| iPhone5s|
     * |51813713|    男| 67|15252507804| foxmail.com|Galaxy S6|
     * |51817665|    女| 32|13850264085| hotmail.com|  iPhone6|
     * |51814981|    男| 95|15331322343|   gmail.com|    小米4|
     * |51817082|    女| 68|13779230142|    yeah.com|    荣耀7|
     * |51812077|    男|  5|15259110517|      126.cn| 小米Note|
     * |51817313|    女| 88|15152148045|    0355.net|    Mate7|
     * |51819665|    女| 97|15240601221|     126.net|    小米4|
     * |51814604|    男| 87|13842585962| foxmail.com|    荣耀7|
     * |51811987|    男| 88|15258844986|     163.com| 小米Note|
     * +--------+------+---+-----------+------------+---------+
     */
    val baseFeatureSQL =
      """
        |select
        |t1.distinct_id as uid,
        |gender,
        |age,
        |case when length(mobile) >= 11 then substring(mobile, -11, length(mobile))
        |else '' end as region,
        |case when size(split(email,"@")) = 2 then split(email, "@")[1]
        |else '' end as email_suffix,
        |model
        |from
        |ods_news.user as t1
        |left join
        |dwb_news.user_base_info as t2
        |on
        |t1.distinct_id = t2.distinct_id
        |""".stripMargin

    // 创建表
    ss.sql(
      """
        |CREATE TABLE if not exists dws_news.user_profile_base(
        |uid string,
        |gender string,
        |age string,
        |region string,
        |email_suffix string,
        |model string) STORED AS PARQUET
        |""".stripMargin)
    // 查询数据生成DF
    val baseFeatureDF = ss.sql(baseFeatureSQL)
    // 缓存DF，一是存储数据在HDFS上，二是写入到ClickHouse中
    baseFeatureDF.cache()
    // 存储一份数据在HDFS上，覆盖写
    baseFeatureDF.write.mode(SaveMode.Overwrite).saveAsTable("dws_news.user_profile_base")

    // 创建ClickHouse表,meta._1 是列名，meta._2是sql占位符
    val meta = TableUtil.genClickhouseUserProfileBaseTable(baseFeatureDF, params)
    // 生成插入数据的Prepare SQL
    // 生成样例如下 insert into app_news.user_profile_base (uid, gender, age, region, email_suffix, model) values (?, ?, ?, ?, ?, ?)
    val insertSQL = {
      s"""
         |insert into ${TableUtil.USER_PROFILE_CLICKHOUSE_TABLE} (${meta._1}) values (${meta._2})
         |""".stripMargin
    }

    log.warn(s"insert sql prepare $insertSQL")

    baseFeatureDF.foreachPartition(partition => {
      TableUtil.insertBaseProfileToClickHouse(partition, insertSQL, params)
    })

    baseFeatureDF.unpersist()
    ss.stop()
    log.warn("Job work success !")
  }
}
