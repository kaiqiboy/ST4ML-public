package operators.selector.partitioner

import instances.{Duration, Event, Extent, Instance}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import operators.selector.SelectionUtils.PartitionInfo
import scala.collection.mutable
import scala.math.{floor, sqrt}
import scala.reflect.ClassTag

class TSTRExtendPartitioner(val dimPartitions: Array[(String, Int)],
                            var samplingRate: Option[Double] = None,
                            ref: String = "center",
                            threshold: Double = 0) {
  val sPartitions: Int = dimPartitions.find(_._1 == "spatial").get._2
  val tPartitions: Int = dimPartitions.find(_._1 == "temporal").get._2
  val numPartitions: Int = sPartitions * tPartitions
  val spark: SparkSession = SparkSession.builder.getOrCreate()
  val tNumPartition: Int = sqrt(numPartitions).toInt
  val sNumPartition: Int = numPartitions / tNumPartition

  def partitionST[T <: Instance[_, _, Map[String, Int]] : ClassTag](dataRDD: RDD[T]): RDD[T] = {
    val temporalPartitioner = new TemporalPartitioner(tNumPartition, samplingRate, ref)
    val tPartitionRdd = temporalPartitioner.partition(dataRDD)
    val tRanges = temporalPartitioner.slots.zipWithIndex.map(_.swap).toMap
    val stRanges = mutable.Map.empty[Int, (Duration, Extent)]
    val tSplitRdd = tPartitionRdd.mapPartitionsWithIndex {
      case (idx, iter) => iter.map(x => (idx, x))
    }
    val sr = samplingRate.getOrElse(1.0)
    for (i <- 0 until tNumPartition) {
      val samples = tSplitRdd.filter(_._1 == i).sample(withReplacement = false, sr).map(_._2).collect
        .map(x => Event(x.spatialCenter, Duration(0))) // the duration is not used, just put 0 to make it instance
      val sRanges = getPartitionRange(samples)
      for (s <- sRanges)
        stRanges += ((i * sNumPartition + s._1) -> (tRanges(i), s._2))
    }
    val idxRDD = dataRDD.map(x => {
      val idxs = stRanges.filter(st => {
        val centroid = x.center
        Event(centroid._1, Duration(centroid._2)).intersects(st._2._2, st._2._1)
      })
      val idx = if (idxs.isEmpty) {
        stRanges.filter(st => st._2._1.intersects(x.duration))
          .mapValues(st => st._2.distance(x.spatialCenter))
          .minBy(_._2)._1
      }
      else idxs.head._1
      (idx, x)
    })
    idxRDD.partitionBy(new KeyPartitioner(numPartitions))
      .map(_._2)
  }

  def partition[T <: Instance[_, _, Map[String, Int]] : ClassTag](dataRDD: RDD[T]): (RDD[(Int, T)], Array[PartitionInfo]) = {
    // result RDD(partitionID, Instance) where the partitionID corresponds to the partitionInfo array
    val otherPartitions = dimPartitions.filter(x => x._1 != "spatial" && x._1 != "temporal")
    val sr = samplingRate.getOrElse(1.0)
    val attrs = otherPartitions.toIterator
    val k = attrs.next
    var (pRDD, _) = partitionByAttr(dataRDD, k._1, k._2, sr)
    while (attrs.hasNext) {
      val k = attrs.next
      pRDD = pRDD.mapPartitions(p => {
        val instances = p.toArray
        val (pArr, _) = partitionByAttr(instances, k._1, k._2, sr)
        Iterator(pArr.map(_._2))
      }).flatMap(x => x)
    }
    val indexed = pRDD.mapPartitionsWithIndex {
      case (idx, partition) => partition.map(x => (idx, x))
    }
    var allRanges = new Array[Map[String, Array[Double]]](0)
    var allRDDs = new Array[RDD[(Int, T)]](0)
    for (i <- Range(0, indexed.getNumPartitions)) {
      val rdd = indexed.filter(_._1 == i)
      val ranges = getRanges(rdd.map(_._2), otherPartitions.map(_._1))
      allRanges = allRanges :+ ranges
      val partitionedRDD = this.partitionST(rdd.map(_._2)).map(x => (i, x))
      allRDDs = allRDDs :+ partitionedRDD
    }
    val unionRDDs = spark.sparkContext.union(allRDDs)
    val metadata = unionRDDs.mapPartitionsWithIndex { case (idx, partition) =>
      val instances = partition.toArray
      val spatialRange = Extent(instances.map(_._2.extent))
      val temporalRange = Array(instances.map(_._2.duration.start).min.toDouble, instances.map(_._2.duration.start).max.toDouble)
      Iterator((idx, allRanges(instances.head._1)
        + ("spatial" -> Array(spatialRange.xMin, spatialRange.yMin, spatialRange.xMax, spatialRange.yMax)) + ("temporal" -> temporalRange), instances.length))
    }.map(x => {
      PartitionInfo(pId = x._1, spatial = x._2("spatial"),
        temporal = x._2("temporal").map(_.toLong),count = x._3, nonST = x._2.-("spatial", "temporal"))
    }).collect
    (unionRDDs.mapPartitionsWithIndex{
      case(id, p) => p.map(x => (id, x._2))
    }, metadata)
  }

  def getRanges[T <: Instance[_, _, Map[String, Int]] : ClassTag](rdd: RDD[T], keys: Array[String]): Map[String, Array[Double]] = {
    var res = Map[String, Array[Double]]()
    for (key <- keys) {
      val attr = rdd.map(_.data(key))
      res = res + (key -> Array(attr.min, attr.max))
    }
    res
  }

  def getPartitionRange[T <: Instance[_, _, _] : ClassTag](instances: Array[T]): Map[Int, Extent] = {
    def getBoundary(df: DataFrame, n: Int, column: String): Array[Double] = {
      val interval = 1.0 / n
      var t: Double = 0
      var q = new Array[Double](0)
      while (t < 1 - interval) {
        t += interval
        q = q :+ t
      }
      q = 0.0 +: q
      if (q.length < n + 1) q = q :+ 1.0
      df.stat.approxQuantile(column, q, 0.001)
    }

    def getStrip(df: DataFrame, range: List[Double], column: String): DataFrame = {
      df.filter(functions.col(column) >= range.head && functions.col(column) < range(1))
    }

    def genBoxes(x_boundaries: Array[Double], y_boundaries: Array[Array[Double]]):
    (Array[List[Double]], Map[List[Double], Array[(List[Double], Int)]]) = {
      var metaBoxes = new Array[List[Double]](0)
      for (x <- 0 to x_boundaries.length - 2) metaBoxes = metaBoxes :+
        List(x_boundaries(x), y_boundaries(x)(0), x_boundaries(x + 1), y_boundaries(0).last)
      var boxMap: Map[List[Double], Array[(List[Double], Int)]] = Map()
      var boxes = new Array[List[Double]](0)
      var n = 0
      for (i <- 0 to x_boundaries.length - 2) {
        var stripBoxes = new Array[(List[Double], Int)](0)
        val x_min = x_boundaries(i)
        val x_max = x_boundaries(i + 1)
        for (j <- 0 to y_boundaries(i).length - 2) {
          val y_min = y_boundaries(i)(j)
          val y_max = y_boundaries(i)(j + 1)
          val box = List(x_min, y_min, x_max, y_max)
          boxes = boxes :+ box
          stripBoxes = stripBoxes :+ (box, n)
          n += 1
        }
        boxMap += metaBoxes(i) -> stripBoxes
      }
      (boxes, boxMap)
    }

    def getWholeRange(df: DataFrame, column: List[String]): Array[Double] = {
      implicit def toDouble: Any => Double = {
        case i: Int => i
        case f: Float => f
        case d: Double => d
      }

      val x_min = df.select(functions.min(column.head)).collect()(0)(0)
      val x_max = df.select(functions.max(column.head)).collect()(0)(0)
      val y_min = df.select(functions.min(column(1))).collect()(0)(0)
      val y_max = df.select(functions.max(column(1))).collect()(0)(0)
      val x_border = (x_max - x_min) * 0.001
      val y_border = (y_max - y_min) * 0.001
      Array(x_min - x_border, y_min - y_border, x_max + x_border, y_max + y_border)
    }

    def replaceBoundary(x_boundaries: Array[Double], y_boundaries: Array[Array[Double]], wholeRange: Array[Double]):
    (Array[Double], Array[Array[Double]]) = {
      val n_x_boundaries = x_boundaries
      var n_y_boundaries = new Array[Array[Double]](0)
      n_x_boundaries(0) = wholeRange(0)
      n_x_boundaries(n_x_boundaries.length - 1) = wholeRange(2)
      for (y <- y_boundaries) {
        val n_y_boundary = wholeRange(1) +: y.slice(1, y.length - 1) :+ wholeRange(3)
        n_y_boundaries = n_y_boundaries :+ n_y_boundary
      }
      (n_x_boundaries, n_y_boundaries)
    }

    def STR(df: DataFrame, columns: List[String], coverWholeRange: Boolean):
    (Array[List[Double]], Map[List[Double], Array[(List[Double], Int)]]) = {
      // columns: sequence of partitioning columns, e.g. List("x", "y") means partition on x first then y
      // return boxes
      val s = floor(sqrt(sNumPartition)).toInt
      val n = floor(sNumPartition / s.toDouble).toInt
      var x_boundaries = getBoundary(df, s, columns.head)
      assert(s * n <= sNumPartition)
      var y_boundaries = new Array[Array[Double]](0)
      for (i <- 0 to x_boundaries.length - 2) {
        val range = List(x_boundaries(i), x_boundaries(i + 1))
        val stripRDD = getStrip(df, range, columns.head)
        val y_boundary = getBoundary(stripRDD, n, columns(1))
        y_boundaries = y_boundaries :+ y_boundary
      }
      if (coverWholeRange) {
        val wholeRange = getWholeRange(df, List("x", "y"))
        val new_boundaries = replaceBoundary(x_boundaries, y_boundaries, wholeRange)
        x_boundaries = new_boundaries._1
        y_boundaries = new_boundaries._2
      }
      genBoxes(x_boundaries, y_boundaries)
    }

    val xy = instances.map(x => (x.spatialCenter.getX, x.spatialCenter.getY))
    val df = spark.createDataFrame(xy).toDF("x", "y").cache()
    val res = STR(df, List("x", "y"), coverWholeRange = true)
    val boxes = res._1
    var boxesWIthID: Map[Int, Extent] = Map()
    for (i <- boxes.indices) {
      val lonMin = boxes(i).head
      val latMin = boxes(i)(1)
      val lonMax = boxes(i)(2)
      val latMax = boxes(i)(3)
      boxesWIthID += (i -> Extent(lonMin, latMin, lonMax, latMax))
    }
    if (threshold != 0) boxesWIthID.mapValues(rectangle => rectangle.expandBy(threshold)).map(identity)
    else boxesWIthID
  }

  def partitionByAttr[T <: Instance[_, _, Map[String, Int]] : ClassTag](dataRDD: RDD[T],
                                                                        attr: String, numPartitions: Int,
                                                                        samplingRate: Double): (RDD[T], Array[(Double, Double)]) = {
    val aRDD = dataRDD.map(x => x.data(attr))
    val sampled = aRDD.sample(false, samplingRate)
    import spark.implicits._
    val df = sampled.toDF
    val wholeRange = (aRDD.min.toDouble, aRDD.max.toDouble)
    val splitPoints = (0 to numPartitions).map(_ / numPartitions.toDouble).toArray
    val ranges = df.stat.approxQuantile("value", splitPoints, 0.01)
      .sliding(2).toArray.map(x => (x(0), x(1))).zipWithIndex
    val rangesExtended = ((wholeRange._1, ranges(0)._1._2), 0) +:
      ranges.drop(1).dropRight(1) :+ ((ranges.last._1._1, wholeRange._2), ranges.last._2)
    val slots = rangesExtended.map(_._1)
    val rddWIdx = dataRDD.map(instance => {
      val overlaps = rangesExtended.filter(x => instance.data(attr) >= x._1._1 && instance.data(attr) <= x._1._2)
      val overlap = if (overlaps.length > 0) overlaps.head._2 else 0
      (overlap, instance)
    })
    val partitioner = new KeyPartitioner(numPartitions)
    (rddWIdx.partitionBy(partitioner).map(_._2), slots)
  }

  def partitionByAttr[T <: Instance[_, _, Map[String, Int]] : ClassTag](data: Array[T], attr: String, numPartitions: Int,
                                                                        samplingRate: Double): (Array[(Int, T)], Array[(Double, Double)]) = {
    import spark.implicits._
    val rdd = spark.sparkContext.parallelize(data.toList).map(_.data(attr))
    val df = rdd.sample(false, samplingRate).toDF
    val wholeRange = (rdd.min.toDouble, rdd.max.toDouble)
    val splitPoints = (0 to numPartitions).map(_ / numPartitions.toDouble).toArray
    val ranges = df.stat.approxQuantile("value", splitPoints, 0.01)
      .sliding(2).toArray.map(x => (x(0), x(1))).zipWithIndex
    val rangesExtended = ((wholeRange._1, ranges(0)._1._2), 0) +:
      ranges.drop(1).dropRight(1) :+ ((ranges.last._1._1, wholeRange._2), ranges.last._2)
    val slots = rangesExtended.map(_._1)
    val arrWIdx = data.map(instance => {
      val overlap = rangesExtended.filter(x => instance.data(attr) >= x._1._1 && instance.data(attr) <= x._1._2).head._2
      (overlap, instance)
    })
    (arrWIdx, slots)
  }

  def partitionTemporal[T <: Instance[_, _, Map[String, Int]] : ClassTag](data: Array[T], numPartitions: Int,
                                                                          samplingRate: Double): (Array[(Int, T)], Array[Duration]) = {
    import spark.implicits._
    val rdd = spark.sparkContext.parallelize(data.toList).map(_.temporalCenter)
    val df = rdd.sample(false, samplingRate).toDF
    val wholeRange = (rdd.min, rdd.max)
    val splitPoints = (0 to numPartitions).map(_ / numPartitions.toDouble).toArray
    val ranges = df.stat.approxQuantile("value", splitPoints, 0.01)
      .sliding(2).toArray.map(x => Duration(x(0).toLong, x(1).toLong)).zipWithIndex
    val rangesExtended = (Duration(wholeRange._1, ranges(0)._1.end), 0) +:
      ranges.drop(1).dropRight(1) :+
      (Duration(ranges.last._1.start, wholeRange._2), ranges.last._2)
    val slots = rangesExtended.map(_._1)
    val arrWIdx = data.map(instance => {
      val overlap = rangesExtended.filter(x => x._1.intersects(instance.temporalCenter)).head._2
      (overlap, instance)
    })
    (arrWIdx, slots)
  }

}


