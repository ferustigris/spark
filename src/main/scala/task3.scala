import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.{SparkContext, SparkConf}
import org.jnetpcap.Pcap
import org.jnetpcap.packet.{PcapPacket, PcapPacketHandler}

object task3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("task3").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(1))

    val data = ssc.receiverStream(new CustomReceiver())
    val words = data.flatMap(_.split(" ")).foreach(println(_))

    ssc.start()
    ssc.awaitTermination()
  }
}
