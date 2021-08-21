package hahaha.lalala.UserProfile

import hahaha.lalala.UserProfile.conf.Config
import hahaha.lalala.UserProfile.util.{SparkHelper, TableUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode
import org.slf4j.LoggerFactory

/**
 * @Description 生成基础特征标签，生成后的标签写入到Clickhouse
 *              1. 性别 2.年龄 3.电话归属地 4. 邮箱尾号 5. 手机机型
 *                 五个基础特征标签
 * @Author niyaolanggeyo
 * @Date 2021/8/20 17:55
 * @Version 1.0
 */
object LabelGenerator2 {
  private val log = LoggerFactory.getLogger("label-generator")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    System.setProperty("HADOOP_USER_NAME", "ly")

    // 解析命令行参数
    val params = Config.parseConfig(LabelGenerator, args)
    log.warn("Job running please wait ... ")

    // init spark session
    val ss = SparkHelper.getSparkSession(params.env, "label-generator")

    val baseFeatureSQL =
      """
        |select
        |t1.distinct_id as uid,
        |gender,
        |age,
        |region,
        |case when size(split(email,"@")) = 2 then split(email,"@")[1] else '' end as email_suffix,
        |model
        |from
        |dwd_news.user as t1
        |left join
        |dwb_news.user_base_info as t2
        |on
        |t1.distinct_id = t2.distinct_id
        |""".stripMargin

    // 1.dws_news.user_profile_base
    // 创建表Hive
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

    // 2.ch:app_news.user_profile_base
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
