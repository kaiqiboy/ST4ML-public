package experiments

import instances.{Duration, Event, Extent, Point}
import operators.selector.SelectionUtils.{LoadPartitionInfo, LoadPartitionInfoLocal}
import operators.selector.Selector
import operators.selector.partitioner.TSTRExtendPartitioner
import org.apache.spark.sql.SparkSession


object TSTRExtendedTest extends App {
  val spark = SparkSession.builder()
    .appName("AnomalyExtraction")
    .master("local[4]")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  /**
   * test partition with tstrExtended metadata
   */

  //  type EVENT = Event[Point, Option[String], String]
  //  val selector = Selector[EVENT](Extent(-8.651553, 41.066949, -7.984440, 41.324905).toPolygon, Duration(1376540750, 1394655756), 16)
  //  val eventRDD = selector.selectEvent("datasets/porto_taxi_point_0.2_tstr", "None", false).sample(false, 0.0001)
  //  println(eventRDD.count)
  //
  //  val reformatRDD = eventRDD.map(x => x.mapData(x => Map("id" -> (x.toLong % 100000).toInt)))
  //  val partitionInfo = Map("spatial" -> 4, "temporal" -> 2, "id" -> 2).toArray
  //  val partitioner = new TSTRExtendPartitioner(partitionInfo, Some(0.2))
  //
  //  val (partitioned, partitionRes) = partitioner.partition(reformatRDD)
  //  partitioned.mapPartitions(x => Iterator(x.size)).collect.foreach(println)
  //  partitionRes.toDisk("datasets/test")
  //  import operators.selector.SelectionUtils._
  //  partitioned.map(x => (x._2, x._1)).toDs(x=>x,x=>x.toString).toDisk("datasets/tstrExtended")


  /**
   * test load with tstrExtended metadata
   */
  type EVENT = Event[Point, Option[String], String]
  val selector = Selector[EVENT](Extent(-8.651553, 41.066949, -7.984440, 41.324905).toPolygon,
    Duration(1376540750, 1394655756), 16, Map("id" -> (5, 25)))
  val eventRDD = selector.selectEvent("datasets/tstrExtended",
    "datasets/test/part-00000-c14ea788-f333-42af-9a31-23d69b0504ab-c000.json", false)
  println(eventRDD.count)
}
