import org.jnetpcap.Pcap
import org.jnetpcap.packet.{PcapPacket, PcapPacketHandler}
;

/**
  * Created by asd on 26.12.15.
  */
object Task3 {
  class PackHandler extends PcapPacketHandler[String] {
    override def nextPacket(pcapPacket: PcapPacket, t: String): Unit = {
      println(t)
      println(pcapPacket.toString)
    }
  }
  def main(args: Array[String]): Unit = {
    var errbuf = new java.lang.StringBuilder();
    var pcap = Pcap.openOffline("pcap.pcap", errbuf)
    var handler = new PackHandler
    pcap.loop(10, handler, "")

  }
}
