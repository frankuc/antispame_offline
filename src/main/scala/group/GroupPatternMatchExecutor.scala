/**

 feature-group
 shuai zhang
 BeiJing, may 16, 2023
 */

package group

import java.util
import java.util.concurrent.{Callable, Executors, Future}
import scala.collection.JavaConverters._

object GroupPatternMatchExecutor {

  def main(argv: Array[String]):Unit = {
    val spark = SparkSession
      .builder()
      .appName("antispam_offline.group.GroupPatternMatchExecutor")
      .enableHiveSupport()
      .getOrCreate()

    val args = new GroupPatternMatchArgs(argv)
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
          val groupPatternMatch = new GroupPatternMatch()
          groupPatternMatch.initialize(groupNodes(i), args)
          groupPatternMatch.groupPatternMatch(spark)
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
