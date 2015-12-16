import org.apache.spark.{SparkContext, SparkConf}
import scala.util.matching.Regex
import scala.util.matching.Regex.MatchIterator

object task1 {
  def getRecord(iter: MatchIterator): Tuple2[String, Int] = {
    (iter.group(0), iter.group(7).toInt)
  }
  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("task1").setMaster("local")
    val sc = new SparkContext(conf);

    // put some data in an RDD
    val f = sc.textFile("000000")
    val hz = f.map(line => getRecord("([^\"]\\S*|\".+?\")\\s*".r.findAllIn(line)))

    val numbersRDD = sc.parallelize(numbers, 4)
    println("Print each element of the original RDD")
    numbersRDD.foreach(println)

    // trivially operate on the numbers
    val stillAnRDD = numbersRDD.map(n => n.toDouble / 10)

    // get the data back out
    val nowAnArray = stillAnRDD.collect()
    // interesting how the array comes out sorted but the RDD didn't
    println("Now print each element of the transformed array")
    nowAnArray.foreach(println)

    // explore RDD properties
    val partitions = stillAnRDD.glom()
    println("We _should_ have 4 partitions")
    println(partitions.count())
    partitions.foreach(a => {
      println("Partition contents:" +
        a.foldLeft("")((s, e) => s + " " + e))
    })
  }
}
