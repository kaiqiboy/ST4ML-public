//package experiments
//
//import instances.{Duration, Event, Extent, Point}
//import operators.converter.Event2TimeSeriesConverter
//import operators.selector.Selector
//import org.apache.spark.sql.SparkSession
//
//import java.lang.System.nanoTime
//
//object TsFlowExtraction {
//  case class E(lon: Double, lat: Double, t: Long, id: String)
//
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder()
//      .appName("PeriodicalFlowExtraction")
//      .master(args(0))
//      .getOrCreate()
//
//    val sc = spark.sparkContext
//    sc.setLogLevel("ERROR")
//
//    val fileName = args(1)
//    val spatialRange = args(2).split(",").map(_.toDouble)
//    val temporalRange = args(3).split(",").map(_.toLong)
//    val numPartitions = args(4).toInt
//
//    val sQuery = new Extent(spatialRange(0), spatialRange(1), spatialRange(2), spatialRange(3))
//    val tQuery = Duration(temporalRange(0), temporalRange(1))
//
//    val selector = Selector[Event[Point, None.type, String]](sQuery, tQuery, numPartitions)
//    val res = selector.selectEvent(fileName)
//
//    val f: Array[Event[Point, None.type, String]] => Int = x => x.length
//    val tArray = (tQuery.start until tQuery.end by 86400)
//      .sliding(2)
//      .map(x => Duration(x(0), x(1))).toArray
//
//    val converter = new Event2TimeSeriesConverter(tArray)
//    val tsRDD = converter.convert(res, f)
//    tsRDD.count
//    val t = nanoTime
//    val binRDD = tsRDD.flatMap(ts => ts.entries.zipWithIndex.map {
//      case (bin, idx) => (idx, bin.value)
//    })
//    val binCount = binRDD.reduceByKey(_ + _).collect
//    val t2 = nanoTime
//
//    println(binCount.deep)
//    println((t2 - t) * 1e-9)
//
//    sc.stop()
//  }
//}
