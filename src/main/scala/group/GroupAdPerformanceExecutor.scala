/**

 feature-group
 shuai zhang
 BeiJing, may 16, 2023
 */

package group

import org.apache.spark.sql.SparkSession
import scala.xml.XML

import java.util
import java.util.concurrent.{Callable, Executors, Future}
import scala.collection.JavaConverters._

object GroupAdPerformanceExecutor {

  def main(argv: Array[String]):Unit = {
    val spark = SparkSession
      .builder()
      .appName("antispam_offline.group.GroupAdPerformanceExecutor")
      .enableHiveSupport()
      .getOrCreate()

    val args = new GroupExecutorArgs(argv)
    val xmlFile = XML.loadFile(args.GroupConfig)

    Tools.featureGroupCheck(xmlFile, args.GroupNames)
    Tools.duplicateGroupNameTest(xmlFile)

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
          val groupGamePerformance = new GroupGamePerformance()
          groupGamePerformance.initialize(groupNodes(i), args)
          groupGamePerformance.compute(spark)

          val groupUGPerformance = new GroupUGPerformance()
          groupUGPerformance.initialize(groupNodes(i), args)
          groupUGPerformance.compute(spark)

          val groupEcomPerformance = new GroupEcomPerformance()
          groupEcomPerformance.initialize(groupNodes(i), args)
          groupEcomPerformance.compute(spark)
        }
      })
      list.add(task)
    }

    list.asScala.foreach( result => {
      println(result.get())
    })

    spark.stop()
  }

}
