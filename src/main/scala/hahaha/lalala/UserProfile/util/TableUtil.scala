package hahaha.lalala.UserProfile.util

import hahaha.lalala.UserProfile.conf.Config
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType}
import org.apache.spark.sql.{DataFrame, Row}
import org.slf4j.{Logger, LoggerFactory}

/**
 * @Description 生成 ClickHouse 数据
 * @Author niyaolanggeyo
 * @Date 2021/8/20 15:17
 * @Version 1.0
 */
object TableUtil {
  private val log: Logger = LoggerFactory.getLogger("TableUtil")

  val USER_PROFILE_CLICKHOUSE_DATABASE = "app_news"
  val USER_PROFILE_CLICKHOUSE_TABLE = "app_news.user_profile_base"


  // 根据DF生成clickhouse表，返回 df的各个列名字和sql占位符，用于写入数据
  def genClickhouseUserProfileBaseTable(baseFeatureDF: DataFrame, params: Config): (String, String) = {
    /**
     * 根据Schema 生成字段，结果类似如下
     * fName = uid, gender, age, region, email_suffix, model
     * fNameType = uid string,gender string,age string,region string,email_suffix string,model string
     * pl = ?, ?, ?, ?, ?, ?
     */
    val (fName, fNameType, pl) = baseFeatureDF.schema.fields.foldLeft("", "", "")(
      (z, f) =>
        if (z._1.nonEmpty && z._2.nonEmpty && z._3.nonEmpty)
          (z._1 + ", " + f.name, z._2 + ", " + f.name + " " + f.dataType.simpleString, z._3 + "?")
        else (f.name, f.name + " " + f.dataType.simpleString, "?")
    )

    val chCol = ClickHouseHelper.dfTypeNameToCH(fNameType)

    val chTableSQL =
      s"""
         |create table $USER_PROFILE_CLICKHOUSE_TABLE($chCol)
         |ENGINE = MergeTree()
         |ORDER BY (uid)
         |""".stripMargin

    val chSource = ClickHouseHelper.getSource(params.username, params.password, params.url)
    val connection = chSource.getConnection

    val createDBSQL =
      s"""
         |create database if not exists $USER_PROFILE_CLICKHOUSE_DATABASE
         |""".stripMargin

    log.warn(createDBSQL)
    var preparedStatement = connection.prepareStatement(createDBSQL)
    preparedStatement.execute()

    val dropTableSQL =
      s"""
         |drop database if exists $USER_PROFILE_CLICKHOUSE_TABLE
         |""".stripMargin

    log.warn(dropTableSQL)
    preparedStatement = connection.prepareStatement(dropTableSQL)
    preparedStatement.execute()

    log.warn(chTableSQL)
    preparedStatement = connection.prepareStatement(chTableSQL)

    preparedStatement.execute()
    preparedStatement.close()
    connection.close()
    log.warn("Init success!")
    (fName, pl)
  }

  // 每个partition建立一个clickhouse连接，写入user_profile_base数据
  def insertBaseProfileToClickHouse(par: Iterator[Row], insertSQL: String, params: Config): Unit = {
    val chSource = ClickHouseHelper.getSource(params.username, params.password, params.url)
    val connection = chSource.getConnection
    val preparedStatement = connection.prepareStatement(insertSQL)
    var batchCount = 0
    val batchSize = 10000
    var lastBatchTime = System.currentTimeMillis
    val batchInterval = 3000

    par.foreach(line => {
      var fieldIndex = 1
      line.schema.fields.foreach(field => {
        field.dataType match {
          case StringType =>
            preparedStatement.setString(fieldIndex, line.getAs[String](field.name))
          case LongType =>
            preparedStatement.setLong(fieldIndex, line.getAs[Long](field.name))
          case IntegerType =>
            preparedStatement.setInt(fieldIndex, line.getAs[Integer](field.name))
          case DoubleType =>
            preparedStatement.setDouble(fieldIndex, line.getAs[Double](field.name))
          case _ =>
            log.error(s"Type to ClickHouse not support : ${field.dataType}")
        }
        fieldIndex += 1
      })

      preparedStatement.addBatch()
      batchCount = batchCount + 1
      if (batchCount >= batchSize || lastBatchTime < System.currentTimeMillis - batchInterval) {
        lastBatchTime = System.currentTimeMillis
        preparedStatement.executeBatch()
        log.warn(s"Send ClickHouse data batch num : $batchCount, batch time :${System.currentTimeMillis - lastBatchTime} ms")
        batchCount = 0
      }
    })
    preparedStatement.executeBatch()
    log.warn(s"Last send ClickHouse data batch num : $batchCount, batch time :${System.currentTimeMillis - lastBatchTime} ms")
    preparedStatement.close()
    connection.close()
  }

}
