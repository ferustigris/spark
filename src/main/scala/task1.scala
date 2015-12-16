import org.apache.spark.{SparkContext, SparkConf}
import scala.util.matching.Regex
import scala.util.matching.Regex.MatchIterator

object task1 {
  def getRecord(iter: MatchIterator): Tuple2[String, Tuple2[Int, Int] ] = {
    (iter.group(0), (1, iter.group(7).toInt))
  }
  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("task1").setMaster("local")
    val sc = new SparkContext(conf);

    // put some data in an RDD
    val f = sc.textFile("000000")
    val records = f.map(line => getRecord("([^\"]\\S*|\".+?\")\\s*".r.findAllIn(line)))
    var res = records.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))

    println("Result:")
    res.foreach(x => print(x._1 + ", " + x._2._1 + ", " + x._2._2))
  }
}
