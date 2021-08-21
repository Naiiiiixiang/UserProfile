package hahaha.lalala.UserProfile.conf


/**
 * @Description：
 * @Author niyaolanggeyo
 * @Date 2021/8/20 13:20
 * @Version 1.0
 */
case class Config(
                   env: String = "",
                   username: String = "",
                   password: String = "",
                   url: String = "",
                   startDate: String = "",
                   endDate: String = "",
                   topK: Int = 30,
                 )

object Config {
  def parseConfig(obj: Object, args: Array[String]): Config = {
    val programName = obj.getClass.getSimpleName.replaceAll("\\$", "")
    val parsar = new scopt.OptionParser[Config]("spark = " + programName) {
      head(programName, "1.0")
      opt[String]('e', "env").required().action((x, config) => config.copy(env = x)).text("env:dev or prod")

      programName match {
        case "LabelGenerator" =>
          opt[String]('n', "username").required().action((x, config) => config.copy(username = x)).text("clickhouse username")
          opt[String]('p', "password").required().action((x, config) => config.copy(password = x)).text("clickhouse password")
          opt[String]('u', "url").required().action((x, config) => config.copy(url = x)).text("clickhouse url, eg. jdbc:clickhouse://xxx.xxx.xxx:xxx")
        case "NewsContentSegment" =>
        case "NewsKeyWords" =>
          opt[Int]('k', "topK").optional().action((x, config) => config.copy(topK = x)).text("Key words top K, default 30")
        case "UserEmbedding" =>
          opt[String]('t', "start_date").required().action((x, config) => config.copy(startDate = x)).text("query event table start date,eg. 20200701")
          opt[String]('d', "end_date").required().action((x, config) => config.copy(endDate = x)).text("query event table  end date, eg. 20200701")
        case _ =>
      }
    }
    parsar.parse(args, Config()) match {
      case Some(conf) => conf
      case None => {
        println("Cannot parse args ！")
        System.exit(-1)
        null
      }
    }


  }
}