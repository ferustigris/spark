import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex.MatchIterator

object task1 {
  def getRecord(iter: MatchIterator): Tuple2[String, Tuple2[Int, Int] ] = {
    val lst = iter.toList
    if (lst.length < 8) {
      return ("", (0, 0))
    }
    (lst(0).trim, (1, lst(7).trim.toInt))
  }

  def parseLine(line: String): MatchIterator = {
    "([^\"]\\S*|\".+?\")\\s*".r.findAllIn(line)
  }

  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("task1").setMaster("local")
    val sc = new SparkContext(conf);

    // put some data in an RDD
    val f = sc.textFile("000001")
    val records = f.map(line => getRecord(parseLine(line)))
    println(records)
    var res = records.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).map(a => (a._1, (a._2._2/a._2._1, a._2._2)))

    println("Result:")
    res.foreach(x => println(x._1 + ", " + x._2._1 + ", " + x._2._2))
  }
}
