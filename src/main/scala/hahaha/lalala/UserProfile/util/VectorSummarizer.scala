package hahaha.lalala.UserProfile.util

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, Row}

/**
 * @Description：
 * @Author niyaolanggeyo
 * @Date 2021/8/20 15:16
 * @Version 1.0
 */
case class VectorSummarizer(f: String) extends Aggregator[Row, MultivariateOnlineSummarizer, Vector] with Serializable {
  override def zero: MultivariateOnlineSummarizer = new MultivariateOnlineSummarizer

  override def reduce(acc: MultivariateOnlineSummarizer, x: Row): MultivariateOnlineSummarizer = acc.add(x.getAs(f))

  override def merge(acc1: MultivariateOnlineSummarizer, acc2: MultivariateOnlineSummarizer): MultivariateOnlineSummarizer = acc1.merge(acc2)

  override def finish(acc: MultivariateOnlineSummarizer): Vector = acc.mean

  override def bufferEncoder: Encoder[MultivariateOnlineSummarizer] = Encoders.kryo[MultivariateOnlineSummarizer]

  override def outputEncoder: Encoder[Vector] = ExpressionEncoder()
}
