package feature

import org.apache.spark.sql.SparkSession
import scala.xml.XML

object MergeExecutor {
  def main(argv: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("batch.feature.MergeExecutor")
      .enableHiveSupport()
      .getOrCreate()

    val args = new MergeExecutorArgs(argv)
    val xmlFile = XML.loadFile(args.mergeConfig)

    args.mergeGroups.map { mergeGroup =>
      val groupNode = (xmlFile \ "merge-group").filter(x => (x \ "merge-name").text.trim == mergeGroup)

      println(s"========= ${mergeGroup} =============")
      Utils.mergeXmlConfCheck(groupNode)

      val merge = new Merger
      merge.initialize(groupNode, args)
      merge.runMerge(spark)

    }

  }

}
