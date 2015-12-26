name := "LearningSpark"

version := "1.0"

fork := true

scalaVersion := "2.11.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.5.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.0"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.5.0"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test"

libraryDependencies += "junit" % "junit" % "4.12" % "test"

libraryDependencies += "jnetpcap" % "jnetpcap" % "1.4.r1425-1f"