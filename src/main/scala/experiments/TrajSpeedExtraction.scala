package experiments

import instances.{Duration, Extent, Trajectory}
import operators.selector.Selector
import org.apache.spark.sql.SparkSession

import java.lang.System.nanoTime
import scala.io.Source

object TrajSpeedExtraction {
  def main(args: Array[String]): Unit = {
    val master = args(0)
    val fileName = args(1)
    val metadata = args(2)
    val queryFile = args(3)
    val numPartitions = args(4).toInt
    val spark = SparkSession.builder()
      .appName("TrajSpeedExtraction")
      .master(master)
      .getOrCreate()

    /**
     * e.g. local[2] datasets/porto_taxi_traj_0.2_tstr datasets/traj_0.2_metadata.json datasets/queries_10.txt 8
     */

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    // read queries
    val f = Source.fromFile(queryFile)
    val ranges = f.getLines().toArray.map(line => {
      val r = line.split(" ")
      (Extent(r(0).toDouble, r(1).toDouble, r(2).toDouble, r(3).toDouble).toPolygon, Duration(r(4).toLong, r(5).toLong))
    })
    val t = nanoTime()
    type TRAJ = Trajectory[Option[String], String]

    for ((spatial, temporal) <- ranges) {
      val selector = Selector[TRAJ](spatial, temporal, numPartitions)
      val trajRDD = selector.selectTraj(fileName, metadata, false)
      val speedRDD = trajRDD.map(x => (x.data, x.consecutiveSpatialDistance("greatCircle").sum / x.duration.seconds * 3.6))
      val res = speedRDD.collect
      trajRDD.unpersist()
      println(res.take(5).deep)
    }
    println(s"Traj speed extraction ${(nanoTime - t) * 1e-9} s")
    sc.stop()
  }
}
