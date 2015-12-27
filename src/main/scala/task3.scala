import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object task3 {
  val Threshold = 1000
  val Limit = 1000

  abstract class NetGlobalState
  case class LimitNorm() extends NetGlobalState
  case class LimitExceeded() extends NetGlobalState
  var curGlobalState: NetGlobalState = new LimitNorm

  abstract class NetLocalState
  case class ThresholdNorm() extends NetLocalState
  case class ThresholdExceeded() extends NetLocalState
  var curState: NetLocalState = new ThresholdNorm


  def updateFunction(newValues: Seq[Integer], runningCount: Option[Integer]): Option[Integer] = {
    val count = runningCount.getOrElse[Integer](0) + newValues.reduce((a,b)=> a + b)
    val prevState = curGlobalState
    curGlobalState = if (count < Limit) new LimitNorm else new LimitExceeded

    if (!curGlobalState.equals(prevState)) {
      println("Global state has been changed to " + curGlobalState)
    }
    Some(count)
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("task3").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint(".")

    val data = ssc.receiverStream(new CustomReceiver())
    var d = data.reduceByWindow((a,b) => a + b, Seconds(1), Seconds(1)).map(a => {
      val prevState = curState
      curState = if (a < Threshold) new ThresholdNorm else new ThresholdExceeded

      if (!curState.equals(prevState)) {
        println("Local state has been changed to " + curState)
      }
      (1, a)
    }
    ).updateStateByKey(updateFunction _)
    .print()

    ssc.start()
    ssc.awaitTermination()
  }
}
