package experiments

import instances._
import instances.onDiskFormats._
import operators.selector.Selector
import operators.selector.SelectionUtils._
import org.apache.spark.sql.SparkSession

import java.lang.System.nanoTime
import scala.math.sqrt

// microbenchmark
object LoadingWithMetaDataExp {

  def main(args: Array[String]): Unit = {
    val master = args(0)
    val fileName = args(1)
    val numPartitions = args(2).toInt
    val metadata = args(3)
    val sRange = args(4).split(",").map(_.toDouble) // -8.446832 41.010165 -7.932837 41.381359
    val tRange = args(5).split(",").map(_.toLong)
    val instance = args(6)
    val ratio = args(7).toDouble
    val useMetadata = args(8).toBoolean

    /**
     * e.g. local[2] datasets/porto_taxi_point_0.2_tstr 16 datasets/point_0.2_metadata.json -8.7,41,-7.87,41.42 1372636800,1404172800 event 0.89 true
     */

    val spark = SparkSession.builder()
      .appName("LoadingWithMetaDataExp")
      .master(master)
      .getOrCreate()
    import spark.implicits._

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val random = new scala.util.Random(1)
    val wholeSpatial = Extent(sRange(0), sRange(1), sRange(2), sRange(3))
    val wholeTemporal = Duration(tRange(0), tRange(1))

    var t = nanoTime()

    val start1 = random.nextDouble * (1 - sqrt(ratio))
    val start2 = random.nextDouble * (1 - sqrt(ratio))
    val start3 = random.nextDouble * (1 - ratio)
    val spatial = Extent(wholeSpatial.xMin + start1 * (wholeSpatial.xMax - wholeSpatial.xMin),
      wholeSpatial.yMin + start2 * (wholeSpatial.yMax - wholeSpatial.yMin),
      wholeSpatial.xMin + (start1 + sqrt(ratio)) * (wholeSpatial.xMax - wholeSpatial.xMin),
      wholeSpatial.yMin + (start2 + sqrt(ratio)) * (wholeSpatial.yMax - wholeSpatial.yMin)).toPolygon
    val temporal = Duration(wholeTemporal.start + (start3 * (wholeTemporal.end - wholeTemporal.start)).toLong,
      wholeTemporal.start + ((start3 + ratio) * (wholeTemporal.end - wholeTemporal.start)).toLong
    )
    if (instance == "event") {
      type EVT = Event[Point, Option[String], String]
      if (useMetadata) {
        // metadata
        t = nanoTime()
        val selector = Selector[EVT](spatial, temporal, numPartitions)
        val rdd1 = selector.selectEvent(fileName, metadata, false)
        println(rdd1.count)
        println(s"metadata: ${(nanoTime() - t) * 1e-9} s.")
      }
      else {
        //no metadata
        t = nanoTime()
        val eventRDD = spark.read.parquet(fileName).drop("pId").as[STEvent].toRdd //.repartition(numPartitions)
        val rdd2 = eventRDD.filter(_.intersects(spatial, temporal))
        println(rdd2.count)
        println(s"no metadata: ${(nanoTime() - t) * 1e-9} s.\n")
        eventRDD.unpersist()
      }
    }

    else if (instance == "traj") {
      type TRAJ = Trajectory[Option[String], String]
      if (useMetadata) {
        // metadata
        val selector = Selector[TRAJ](spatial, temporal, numPartitions)
        val t = nanoTime()
        val rdd1 = selector.selectTraj(fileName, metadata, false)
        println(rdd1.count)
        println(s"total time: ${(nanoTime() - t) * 1e-9} s.\n")
      }
      // no metadata
      else {
        t = nanoTime()
        val trajDf = spark.read.parquet(fileName).drop("pId").as[STTraj]
        val trajRDD = trajDf.toRdd //.repartition(numPartitions)
        val rdd2 = trajRDD.filter(_.intersects(spatial, temporal))
        println(rdd2.count)
        println(s"no metadata selection time: ${(nanoTime() - t) * 1e-9} s.\n")
      }
    }
    spark.stop()
  }
}
