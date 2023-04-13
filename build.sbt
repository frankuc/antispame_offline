compileOrder := CompileOrder.JavaThenScala

lazy val commonSettings = Seq(
  organization := "dataAnomaly",
  version := "0.1.0",
  scalaVersion := "2.12.10"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "picidae",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1" % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1" % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.0.1" % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.0.1" % "provided",
    libraryDependencies += "org.json4s" %% "json4s-native" % "3.5.0",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test",
    libraryDependencies += "org.scalanlp" %% "breeze" % "0.13.2",
    libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.40",
    libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.6",
    libraryDependencies += "com.hadoop.compression" % "hadoop-gpl-compression" % "0.1.0",
    libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.11" ,
    libraryDependencies += "org.json4s" %% "json4s-native" % "3.3.0"
  )

parallelExecution in Test := false