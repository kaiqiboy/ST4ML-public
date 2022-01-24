package experiments

import instances._
import operators.converter._
import instances.onDiskFormats._
import operators.selector.partitioner.HashPartitioner
import org.apache.spark.sql.SparkSession
import operators.selector.SelectionUtils._

import java.lang.System.nanoTime

// microbenchmark
object ConversionExp {
  def main(args: Array[String]): Unit = {
    val master = args(0)
    val fileName = args(1)
    val numPartitions = args(2).toInt
    val m = args(3) // mode

    /**
     * 1 for event to spatial map
     * 2 for traj to spatial map
     * 3 for event to raster
     * 4 for traj to raster
     * 5 for event to time series
     * 6 for traj to time series
     * 7 for rtree test
     */

    /**
     * e.g. local[2] datasets/porto_taxi_point_0.2_tstr 4 1 -8.7,41,-7.87,41.42 1372636800,1404172800 110 110 50  false
     */

    val sRange = args(4).split(",").map(_.toDouble)
    val tRange = args(5).split(",").map(_.toLong)
    val xSplit = args(6).toInt
    val ySplit = args(7).toInt
    val tSplit = args(8).toInt
    val useRTree = args(9).toBoolean

    val spark = SparkSession.builder()
      .appName("ConversionExp")
      .master(master)
      .getOrCreate()
    import spark.implicits._

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    if (m == "1") {
      val inputRDD = spark.read.parquet(fileName).drop("pId").as[STEvent]
        .toRdd.map(_.asInstanceOf[Event[Point, None.type, String]])
      val partitioner = new HashPartitioner(numPartitions)
      val selectedRDD = partitioner.partition(inputRDD)
      println(selectedRDD.count)
      val t = nanoTime()
      val ranges = splitSpatial(sRange, xSplit, ySplit)
      val converter = if (useRTree) new Event2SpatialMapConverter(ranges) else new Event2SpatialMapConverter(ranges, "regular")
      val convertedRDD = converter.convert(selectedRDD)
      println(convertedRDD.count)
      println(s"Conversion time: ${(nanoTime - t) * 1e-9} s")
    }

    else if (m == "2") {
      type TRAJ = Trajectory[None.type, String]
      val inputRDD = spark.read.parquet(fileName).drop("pId").as[STTraj]
        .toRdd.map(_.asInstanceOf[TRAJ])
      val partitioner = new HashPartitioner(numPartitions)
      val selectedRDD = partitioner.partition(inputRDD)
      println(selectedRDD.count)
      val t = nanoTime()
      val ranges = splitSpatial(sRange, xSplit, ySplit)
      val converter = if (useRTree) new Traj2SpatialMapConverter(ranges) else new Traj2SpatialMapConverter(ranges, "regular")
      val convertedRDD = converter.convert(selectedRDD)
      println(convertedRDD.count)
      println(s"Conversion time: ${(nanoTime - t) * 1e-9} s")

    }
    else if (m == "3") {
      val inputRDD = spark.read.parquet(fileName).drop("pId").as[STEvent]
        .toRdd.map(_.asInstanceOf[Event[Point, None.type, String]])
      val partitioner = new HashPartitioner(numPartitions)
      val selectedRDD = partitioner.partition(inputRDD)
      println(selectedRDD.count)
      val t = nanoTime()
      val sRanges = splitSpatial(sRange, xSplit, ySplit)
      val tRanges = splitTemporal(tRange, tSplit)
      val stRanges = for (s <- sRanges; t <- tRanges) yield (s, t)
      val converter = if (useRTree) new Event2RasterConverter(stRanges.map(_._1), stRanges.map(_._2))
      else new Event2RasterConverter(stRanges.map(_._1), stRanges.map(_._2), "regular")
      val convertedRDD = converter.convert(selectedRDD)
      println(convertedRDD.count)
      println(s"Conversion time: ${(nanoTime - t) * 1e-9} s")
    }
    else if (m == "4") {
      val inputRDD = spark.read.parquet(fileName).drop("pId").as[STTraj]
        .toRdd.map(_.asInstanceOf[Trajectory[None.type, String]])
      val partitioner = new HashPartitioner(numPartitions)
      val selectedRDD = partitioner.partition(inputRDD)
      println(selectedRDD.count)
      val t = nanoTime()
      val sRanges = splitSpatial(sRange, xSplit, ySplit)
      val tRanges = splitTemporal(tRange, tSplit)
      val stRanges = for (s <- sRanges; t <- tRanges) yield (s, t)
      val converter = if (useRTree) new Traj2RasterConverter(stRanges.map(_._1), stRanges.map(_._2))
      else new Traj2RasterConverter(stRanges.map(_._1), stRanges.map(_._2), "regular")
      val convertedRDD = converter.convert(selectedRDD)
      println(convertedRDD.count)
      println(s"Conversion time: ${(nanoTime - t) * 1e-9} s")
    }

    else if (m == "5") {
      val inputRDD = spark.read.parquet(fileName).drop("pId").as[STEvent]
        .toRdd.map(_.asInstanceOf[Event[Point, None.type, String]])
      val partitioner = new HashPartitioner(numPartitions)
      val selectedRDD = partitioner.partition(inputRDD)
      println(selectedRDD.count)
      val t = nanoTime()
      val tRanges = splitTemporal(tRange, tSplit)
      val converter = if (useRTree) new Event2TimeSeriesConverter(tRanges) else new Event2TimeSeriesConverter(tRanges, "regular")
      val convertedRDD = converter.convert(selectedRDD)
      println(convertedRDD.count)
      println(s"Conversion time: ${(nanoTime - t) * 1e-9} s")
    }
    else if (m == "6") {
      val inputRDD = spark.read.parquet(fileName).drop("pId").as[STTraj]
        .toRdd.map(_.asInstanceOf[Trajectory[None.type, String]])
      val partitioner = new HashPartitioner(numPartitions)
      val selectedRDD = partitioner.partition(inputRDD)
      println(selectedRDD.count)
      val t = nanoTime()
      val tRanges = splitTemporal(tRange, tSplit)
      val converter = if (useRTree) new Traj2TimeSeriesConverter(tRanges) else new Traj2TimeSeriesConverter(tRanges, "regular")
      val convertedRDD = converter.convert(selectedRDD)
      println(convertedRDD.count)
      println(s"Conversion time: ${(nanoTime - t) * 1e-9} s")
    }
    sc.stop
  }

  def splitSpatial(spatialRange: Array[Double], xSplit: Int, ySplit: Int): Array[Polygon] = {
    val xMin = spatialRange(0)
    val yMin = spatialRange(1)
    val xMax = spatialRange(2)
    val yMax = spatialRange(3)
    val xStep = (xMax - xMin) / xSplit
    val xs = (0 to xSplit).map(x => x * xStep + xMin).sliding(2).toArray
    val yStep = (yMax - yMin) / ySplit
    val ys = (0 to ySplit).map(y => y * yStep + yMin).sliding(2).toArray
    for (x <- xs; y <- ys) yield Extent(x(0), y(0), x(1), y(1)).toPolygon
  }

  def splitTemporal(temporalRange: Array[Long], tSplit: Int): Array[Duration] = {
    val tMin = temporalRange(0)
    val tMax = temporalRange(1)
    val tStep = (tMax - tMin) / tSplit
    val ts = (0 to tSplit).map(x => x * tStep + tMin).sliding(2).toArray
    for (t <- ts) yield Duration(t(0), t(1))
  }
}
