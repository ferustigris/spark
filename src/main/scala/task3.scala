import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object task3 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("task3").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint(".")

    val data = ssc.receiverStream(new CustomReceiver())
    data.reduceByWindow((a,b) => a + b, Seconds(1), Seconds(1)).map(a => {
      a match {
        case n: Integer if n < 100 => println("ThresholdNorm " + n)
        case n: Integer if n >= 100 => println("ThresholdExceeded " + n)
      }
    }
    )
    .print()

    ssc.start()
    ssc.awaitTermination()
  }
}
