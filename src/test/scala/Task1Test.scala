import org.apache.spark.{SparkContext, SparkConf}
import org.junit.Test
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Task1Test extends FlatSpec with Matchers {
  val line = "ip8 - - [24/Apr/2011:04:34:20 -0400] \"GET /sgi_indy/indy_hd.jpg HTTP/1.1\" 200 17475 \"http://host2/sgi_indy/\" \"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:2.0) Gecko/20100101 Firefox/4.0\""
  "" should "" in {
    val conf = new SparkConf().setAppName("task1").setMaster("local")
    val sc = new SparkContext(conf);

    val i = task1.parseLine(line)
    val lst = i.toList

    assert(lst(0).trim.equals("ip8"))
    assert(lst(7).trim.equals("17475"))

    val rec = task1.getRecord(task1.parseLine(line))

    assert(rec._1.equals("ip8"))
    assert(rec._2._1 == 1)
    assert(rec._2._2 == 17475)
  }

}
