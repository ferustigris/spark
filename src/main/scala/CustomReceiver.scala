import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.jnetpcap.Pcap
import org.jnetpcap.packet.{PcapPacket, PcapPacketHandler}

class CustomReceiver extends Receiver[Integer](StorageLevel.MEMORY_ONLY) with Logging {
  class PackHandler extends PcapPacketHandler[String] {
    override def nextPacket(pcapPacket: PcapPacket, t: String): Unit = {
      Thread sleep(500)
      println("==>")
      println(pcapPacket.getCaptureHeader.caplen())
      println("=>")

      store(pcapPacket.getCaptureHeader.caplen())
    }
  }

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
  }

  private def receive() {
    var errbuf = new java.lang.StringBuilder();
    var pcap = Pcap.openOffline("pcap.pcap", errbuf)
    var handler = new PackHandler
    //pcap.loop(10, handler, "")
    while (true) {
      store(100)
      Thread sleep(500)
    }
  }

}
