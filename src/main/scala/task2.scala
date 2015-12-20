import org.apache.spark.{SparkConf, SparkContext}

object task2 {
  final val MonthField: Int = 1
  final var CountryField: Int = 4
  final var iataField: Int = 0
  final var CarrierField: Int = 8
  final var CarrierIDField: Int = 0
  final var AiroportToField: Int = 17
  final var AiroportFromField: Int = 16
  final var CityField: Int = 3

  def parseFlight(line: String): (String, Array[String]) = {
    val f = line.split(",")
    (f(CarrierField), f)
  }

  def isFlightBetweenJanAndAug(a :(String, Array[String])): Boolean = {
    try {
      a._2(MonthField).toInt > 0 && a._2(MonthField).toInt < 9
    } catch {
      case e :NumberFormatException => false
    }
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("task2").setMaster("local")
    val sc = new SparkContext(conf);

    // put some data in an RDD
    val c = sc.textFile("carriers.csv").map(line => line.replace("\"", "").split(","))
    val f = sc.textFile("2007.csv").map(line => line.split(","))

    println("6. Count total number of flights per carrier in 2007:")
    val fbyc = c.map(line => (line(CarrierIDField), 1))
      .reduceByKey((a,b) => a + b)

    fbyc.foreach(line => {
      print(line._1);
      print("=");
      println(line._2);
    })

    //7. The total number of flights served in Jun 2007 by NYC
    // (all airports, use join with Airports data). 
    // How many stages in jobs where instanced for this query?
    println("7. The total number of flights served in Jun 2007 by NYC")
    val a = sc.textFile("airports.csv").map(line => line.replace("\"", "").split(","))
    val fa = f.flatMap(line => {
      List(
        (line(AiroportFromField), line),
        (line(AiroportToField), line)
      )
    })
    val NYCount = fa.join(a.filter(line => line(CityField).equals("NY")).map(line => (line(0), line)))
      .map(l => 1).reduce((a,b) => a + b)

    println(NYCount)

    //8. Find five most busy airports in US during Jun 01 - Aug 31.
    println("8. Find five most busy airports in US during Jun 01 - Aug 31.")
    fa.filter(isFlightBetweenJanAndAug)
      .join(a.filter(l => l(CountryField).equals("USA")).map(l => (l(iataField), 0)))
      .map(a => (a._1, 1)).reduceByKey((a,b) => a + b)
      .sortBy(a => -a._2).take(5)
      .foreach(l => println(l._1 + " = " + l._2))

    // 9. Find the carrier who served the biggest number of flights
    println("9. Find the carrier who served the biggest number of flights")
    c.map(line => (line(CarrierIDField), 1))
      .join(f.map(line => (line(CarrierField), 1)))
      .map(l => (l._1, 1)).reduceByKey((a, b) => a + b)
      .sortBy(a => -a._2).take(1)
      .foreach(line => {
      print(line._1);
      print("=");
      println(line._2);
    })

  }
}
