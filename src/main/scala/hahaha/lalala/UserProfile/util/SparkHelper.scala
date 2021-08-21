package hahaha.lalala.UserProfile.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @Description 根据环境变量参数创建Spark Session
 * @Author niyaolanggeyo
 * @Date 2021/8/20 15:17
 * @Version 1.0
 */
object SparkHelper {

  def getSparkSession(env: String, appName: String) = {
    env match {
      case "prod" => {
        val conf = new SparkConf()
          .setAppName(appName)
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.sql.hive.metastore.version", "3.1.2")
          .set("spark.sql.cbo.enabled", "true") // Spark-SQL性能优化
          .set("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.enable", "true") // 客户端在写失败的时候，是否使用更换策略，默认是true
          .set("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER") // 默认在3个或以上备份的时候，会尝试更换DN结点写入
          .set("spark.debug.maxToStringFields", "200")

        SparkSession
          .builder()
          .config(conf)
          .enableHiveSupport()
          .getOrCreate()
      }

      case "dev" => {
        val conf = new SparkConf()
          .setAppName(appName + " DEV")
          .setMaster("local[6]")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.sql.hive.metastore.version", "3.1.2")
          .set("spark.sql.cbo.enabled", "true")
          .set("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.enable", "true")
          .set("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
          .set("spark.debug.maxToStringFields", "200")

        SparkSession
          .builder()
          .config(conf)
          .enableHiveSupport()
          .getOrCreate()
      }

      case _ => {
        println("Not match env, exists")
        System.exit(-1)
        null
      }
    }
  }

}
