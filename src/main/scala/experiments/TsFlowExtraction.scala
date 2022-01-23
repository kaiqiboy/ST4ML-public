package experiments

import instances.{Duration, Event, Extent, Point}
import operators.converter.Event2TimeSeriesConverter
import operators.selector.Selector
import org.apache.spark.sql.SparkSession

import java.lang.System.nanoTime
import scala.io.Source

object TsFlowExtraction {
  case class E(lon: Double, lat: Double, t: Long, id: String)

  /**
   * e.g. local[2] datasets/porto_taxi_point_0.2_tstr datasets/point_0.2_metadata.json datasets/queries_10.txt 3600 16
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TsFlowExtraction")
      .master(args(0))
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val fileName = args(1)
    val metadata = args(2)
    val queryFile = args(3)
    val tSplit = args(4).toInt
    val numPartitions = args(4).toInt

    // read queries
    val f = Source.fromFile(queryFile)
    val ranges = f.getLines().toArray.map(line => {
      val r = line.split(" ")
      (Extent(r(0).toDouble, r(1).toDouble, r(2).toDouble, r(3).toDouble).toPolygon, Duration(r(4).toLong, r(5).toLong))
    })
    val t = nanoTime()
    type EVENT = Event[Point, None.type, String]

    for ((spatial, temporal) <- ranges) {
      val selector = Selector[EVENT](spatial, temporal, numPartitions)
      val eventRDD = selector.selectEvent(fileName, metadata, false)
      val tRanges = splitTemporal(Array(temporal.start, temporal.end), tSplit)
      val converter = new Event2TimeSeriesConverter(tRanges)
      val f: Array[Event[Point, None.type, String]] => Int = _.length
      val tsRDD = converter.convert(eventRDD, f)
      val res = tsRDD.collect()

      def valueMerge(x: Int, y: Int): Int = x + y

      val mergedTs = res.drop(1).foldRight(res.head)(_.merge(_, valueMerge, (_, _) => None))
      eventRDD.unpersist()
      println(mergedTs.entries.map(_.value).deep)
    }
    println(s"Anomaly extraction ${(nanoTime - t) * 1e-9} s")
    sc.stop()
  }

  def splitTemporal(temporalRange: Array[Long], tStep: Int): Array[Duration] = {
    val tMin = temporalRange(0)
    val tMax = temporalRange(1)
    val tSplit = ((tMax - tMin) / tStep).toInt
    val ts = (0 to tSplit).map(x => x * tStep + tMin).sliding(2).toArray
    for (t <- ts) yield Duration(t(0), t(1))
  }
}
