package group

import org.apache.spark.sql.SparkSession
import scala.xml.NodeSeq

case class CountingItem(fKey: String, space: String) {
  override def toString: String = {
    s"${fKey}-${space}"
  }
}

class GroupAggregator extends Serializable {

  def initialize() = {
    println("hello initialize()")
  }
  def sqlQuery(): String = {

    ""
  }
  def compute(spark:SparkSession): Unit = {
    println("hello compute()")
  }

}
