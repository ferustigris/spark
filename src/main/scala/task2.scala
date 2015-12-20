import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex.MatchIterator

object task2 {
  def getRecord(iter: MatchIterator): Tuple2[String, Tuple2[Int, Int] ] = {
    val lst = iter.toList
    if (lst.length < 8) {
      return ("", (0, 0))
    }
    (lst(0).trim, (1, lst(7).trim.toInt))
  }

  def parseFlight(line: String): (String, Array[String]) = {
    val f = line.split(",")
    (f(8), f)
  }

  def parseCarrier(line: String): (String, Array[String]) = {
    val f = line.split(",")
    (f(8), f)
  }

  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("task2").setMaster("local")
    val sc = new SparkContext(conf);

    // put some data in an RDD
    val c = sc.textFile("carriers.csv").map(line => line.replace("\"", "").split(","))
    val f = sc.textFile("2007.1.csv").map(line => line.split(","))

    println("Count total number of flights per carrier in 2007:")
    c.map(line => (line(0), line)).join(f.map(line => (line(8), line)))
    .map(l => (l._1, 1))
    .reduceByKey((a,b) => a + b)
    .foreach(line => {
      print(line._1);
      print("=");
      println(line._2);
    })


    println("Result:")
  }
}
