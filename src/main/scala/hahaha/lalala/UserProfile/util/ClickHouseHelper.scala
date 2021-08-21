package hahaha.lalala.UserProfile.util

import ru.yandex.clickhouse.ClickHouseDataSource
import ru.yandex.clickhouse.settings.ClickHouseProperties

/**
 * @Description clickhouse datasource
 * @Author niyaolanggeyo
 * @Date 2021/8/20 15:15
 * @Version 1.0
 */
object ClickHouseHelper {
  def getSource(user: String, password: String, url: String): ClickHouseDataSource = {
    Class.forName("ru.yandex.clickhouse.ClickHouseDriver")
    val properties = new ClickHouseProperties()
    properties.setUser(user)
    properties.setPassword(password)
    val dataSource = new ClickHouseDataSource(url, properties)
    dataSource
  }

  // DataType 数据类型转换为ClickHouse 数据类型
  def dfTypeToCHType(dfTypeStr: String): String = {
    dfTypeStr.toLowerCase match {
      case "string" => "String"
      case "integer" => "Int32"
      case "long" => "Int64"
      case "float" => "Float32"
      case "double" => "Float64"
      case "date" => "Date"
      case "timestamp" => "DateTime"
      case _ => "String"
    }
  }

  def dfTypeNameToCH(dfCol: String): String = {
    dfCol.split(",").map(line => {
      val col = line.split(" ")
      val chType = dfTypeToCHType(col(1))
      val name = col(0)
      name + " " + chType
    }).mkString(",")
  }

}
