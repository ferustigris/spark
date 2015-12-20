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

    //7. The total number of flights served in Jun 2007 by NYC
    // (all airports, use join with Airports data). 
    // How many stages in jobs where instanced for this query?
    println("The total number of flights served in Jun 2007 by NYC")
    val a = sc.textFile("airports.csv").map(line => line.replace("\"", "").split(","))
    val fa = f.flatMap(line => {
      List(
        (line(16), line),
        (line(17), line)
      )
    })
    val NYCount = fa.join(a.filter(line => line(3).equals("NY")).map(line => (line(0), line)))
      .map(l => 1).reduce((a,b) => a + b)

    println(NYCount)

    //8. Find five most busy airports in US during Jun 01 - Aug 31.
    fa.join(a.map(l => (l(0), 0)))
      .map(a => (a._1, 1)).reduceByKey((a,b) => a + b)
      .sortBy(a => -a._2).take(5)
      .foreach(l => println(l._1 + " = " + l._2))
  }
}
