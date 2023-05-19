/**

 feature-group
 shuai zhang
 BeiJing, may 16, 2023
 */

package group

import org.apache.spark.sql.SparkSession
import scala.xml.XML

import java.util.concurrent.{Callable, Executors, Future}
import java.util

object GroupExecutor {

  def main(argv: Array[String]):Unit = {
    val spark = SparkSession
      .builder()
      .appName("antispam_offline.group.GroupExecutor")
      .enableHiveSupport()
      .getOrCreate()

    val args = new GroupExecutorArgs(argv)
    val xmlFile = XML.loadFile(args.GroupConfig)

    val groupNodes =  args.GroupNames.map { groupName =>
      val groupNode = (xmlFile \ "feature-group").filter(x => (x \ "group").text == groupName)
      groupNode
    }

    val threadNums = groupNodes.length
    val list = new util.ArrayList[Future[Unit]]()

    val executors = Executors.newFixedThreadPool(threadNums)
    for(i <- 0 until threadNums) {
      val task = executors.submit(new Callable[Unit] {
        override def call():Unit = {
          val groupAggregator = new GroupAggregator()
          groupAggregator.initialize(groupNodes(i), args)
          groupAggregator.compute(spark)
        }
      })
      list.add(task)
    }

    spark.stop()
  }

}
