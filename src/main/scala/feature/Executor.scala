/**

 feature
 shuai zhang
 BeiJing
 dec 29, 2022
*/

package feature

import org.apache.spark.sql.SparkSession
import scala.xml.XML
import java.util.concurrent.{Callable, Executors, Future}
import java.util
import scala.collection.JavaConverters._

object Executor {

  def main(argv: Array[String]):Unit = {

    val spark = SparkSession
      .builder()
      .appName("batch.feature.ExecutorMulti")
      .enableHiveSupport()
      .getOrCreate()

    val args = new ExecutorArgs(argv)
    val xmlFile = XML.loadFile(args.featureConfig)

    args.featureGroup.map { groupName =>
      val groupNode = (xmlFile \ "feature-group").filter(x => (x \ "group").text == groupName)
      val aggTypes = (groupNode \ "feature" \ "aggregator" \ "type").map(_.text.trim).distinct

      println(s"========= ${groupName} =============")
      Utils.featureXmlConfCheck(groupNode)

      val list = new util.ArrayList[Future[Unit]]()
      val threadNums = aggTypes.length
      val executors = Executors.newFixedThreadPool(threadNums)
      for(i <- 0 until threadNums) {
        val task = executors.submit(new Callable[Unit] {
          override def call():Unit = {
            val aggregator = AggregatorFactory.get(aggTypes(i))
            aggregator.initialize(groupNode, args)
            aggregator.compute(spark)
          }
        })
        list.add(task)
      }

      list.asScala.foreach( result => {
        println(result.get())
      })

    }

    spark.stop()
  }

}
