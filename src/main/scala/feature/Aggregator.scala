package feature

import scala.xml.NodeSeq
import org.apache.spark.sql.SparkSession

trait Aggregator extends Serializable {
  def initialize(xmlNode: NodeSeq, args:ExecutorArgs):Unit
  def compute(spark:SparkSession):Unit
}


object AggregatorFactory {
  def get(typeName: String): Aggregator = {
    val prefix = typeName.split("_").map(_.capitalize).mkString("")
    val className = s"feature.${prefix}Aggregator"

    Class.forName(className)
      .getDeclaredConstructor()
      .newInstance()
      .asInstanceOf[Aggregator]
  }

}
