import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object task3 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("task3").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(1))

    val data = ssc.receiverStream(new CustomReceiver())
    data.reduceByWindow((a,b) => a + b, Seconds(1), Seconds(1))
    .print()

    ssc.start()
    ssc.awaitTermination()
  }
}
