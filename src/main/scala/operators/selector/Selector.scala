package operators.selector

import instances._
import operators.selector.SelectionUtils._
import operators.selector.partitioner.{HashPartitioner, STPartitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.locationtech.jts.geom.Polygon
import instances.onDiskFormats._

import scala.collection.mutable
import scala.reflect.ClassTag

class Selector[I <: Instance[_, _, _] : ClassTag](sQuery: Polygon,
                                                  tQuery: Duration,
                                                  partitioner: STPartitioner,
                                                  nonSTQuery: Map[String, (Double, Double)] = Map[String, (Double, Double)]()) extends Serializable {
  val spark: SparkSession = SparkSession.builder.getOrCreate()

  val parallelism: Int = partitioner.numPartitions

  def loadDf(dataDir: String, metaDataDir: String): DataFrame = {
    val metaData = LoadPartitionInfoLocal(metaDataDir)
    var relatedPartitions = metaData.filter(x =>
      x._2.intersects(sQuery)
        && x._3.intersects(tQuery)
        && x._4 > 0)
    if (nonSTQuery.nonEmpty && nonSTQuery.keySet.forall(x => metaData.head._5.keySet.contains(x))) {
      nonSTQuery.foreach { q =>
        relatedPartitions = relatedPartitions.filter { partition =>
          val map = partition._5
          val pRange = map(q._1).asInstanceOf[List[Double]]
          val qRange = q._2
          qRange._1 <= pRange(1) && qRange._2 >= pRange(0)
        }
      }
    }
    val dirs = relatedPartitions.map(x => dataDir + s"/pId=$x")
    if (dirs.length == 0) throw new AssertionError("No data fulfill the ST requirement.")
    spark.read.parquet(dataDir).filter(col("pId").isin(relatedPartitions.map(_._1): _*))
  }

  def loadDf(dataDir: String): DataFrame = spark.read.parquet(dataDir)


  def selectTraj(dataDir: String, metaDataDir: String = "None", partition: Boolean = true): RDD[I] = {

    import spark.implicits._

    val pInstanceDf = if (metaDataDir == "None") loadDf(dataDir) else loadDf(dataDir, metaDataDir)
    val pInstanceRDD = pInstanceDf.as[STTraj].toRdd
    if (partition) {
      val res = pInstanceRDD.filter(_.intersects(sQuery, tQuery))
      if (nonSTQuery.nonEmpty) res.map(x => x.mapData(ParseMapString(_))).filter(x => {
        nonSTQuery.forall(q => {
          val p = x.data(q._1)
          q._2._1 <= p && q._2._2 >= p
        })
      }).stPartition(partitioner).map(_.asInstanceOf[I])
      else res.stPartition(partitioner).map(_.asInstanceOf[I])
    }
    else {
      val res = pInstanceRDD.filter(_.intersects(sQuery, tQuery))
      if (nonSTQuery.nonEmpty) res.map(x => x.mapData(ParseMapString(_))).filter(x => {
        nonSTQuery.forall(q => {
          val p = x.data(q._1)
          q._2._1 <= p && q._2._2 >= p
        })
      }).map(_.asInstanceOf[I])
      else res.map(_.asInstanceOf[I])
    }
  }

  def selectEvent(dataDir: String, metaDataDir: String = "None", partition: Boolean = true): RDD[I] = {

    import spark.implicits._

    val pInstanceDf = if (metaDataDir == "None") loadDf(dataDir) else loadDf(dataDir, metaDataDir)
    val pInstanceRDD = pInstanceDf.as[STEvent].toRdd
    if (partition) {
      val res = pInstanceRDD.filter(_.intersects(sQuery, tQuery))
      if (nonSTQuery.nonEmpty) res.map(x => x.mapData(ParseMapString(_))).filter(x => {
        nonSTQuery.forall(q => {
          val p = x.data(q._1)
          q._2._1 <= p && q._2._2 >= p
        })
      }).stPartition(partitioner).map(_.asInstanceOf[I])
      else res.stPartition(partitioner).map(_.asInstanceOf[I])
    }
    else {
      val res = pInstanceRDD.filter(_.intersects(sQuery, tQuery))
      if (nonSTQuery.nonEmpty) res.map(x => x.mapData(ParseMapString(_))).filter(x => {
        nonSTQuery.forall(q => {
          val p = x.data(q._1)
          q._2._1 <= p && q._2._2 >= p
        })
      }).map(_.asInstanceOf[I])
      else res.map(_.asInstanceOf[I])
    }
  }

  def select(dataDir: String, metaDataDir: String): RDD[I] = {

    import spark.implicits._

    val pInstanceDf = loadDf(dataDir, metaDataDir)
    val pInstanceRDD = pInstanceDf.head(1).head.get(0) match {
      case _: String => pInstanceDf.as[STEvent].toRdd
      case _: mutable.WrappedArray[_] => pInstanceDf.as[STTraj].toRdd
      case _ => throw new ClassCastException("instance type not supported.")
    }
    val partitionedRDD = if (pInstanceRDD.getNumPartitions < parallelism)
      pInstanceRDD.stPartition(partitioner)
    else pInstanceRDD
    val rdd1 = partitionedRDD
      .filter(_.intersects(sQuery, tQuery))
      .map(_.asInstanceOf[I])
    rdd1
  }

  def select[A: ClassTag](dataDir: String, metaDataDir: String): RDD[A] = {

    import spark.implicits._

    val pInstanceDf = loadDf(dataDir, metaDataDir)
    val pInstanceRDD = pInstanceDf.head(1).head.get(0) match {
      case _: String => pInstanceDf.as[STEvent].toRdd
      case _: mutable.WrappedArray[_] => pInstanceDf.as[STTraj].toRdd
      case _ => throw new ClassCastException("instance type not supported.")
    }
    val partitionedRDD = if (pInstanceRDD.getNumPartitions < parallelism)
      pInstanceRDD.stPartition(partitioner)
    else pInstanceRDD
    val rdd1 = partitionedRDD
      .filter(_.intersects(sQuery, tQuery))
      .map(_.asInstanceOf[A])
    rdd1
  }
}

object Selector {
  def apply[I <: Instance[_, _, _] : ClassTag](sQuery: Polygon,
                                               tQuery: Duration,
                                               numPartitions: Int,
                                               nonSTQuery: Map[String, (Double, Double)] = Map[String, (Double, Double)]()): Selector[I] = {
    new Selector[I](sQuery, tQuery, new HashPartitioner(numPartitions), nonSTQuery)
  }

  //  def apply[I <: Instance[_, _, _] : ClassTag](sQuery: Polygon,
  //                                               tQuery: Duration,
  //                                               partitioner: STPartitioner,
  //                                               nonSTQuery: Map[String, (Double, Double)] = Map[String, (Double, Double)]()): Selector[I] = {
  //    new Selector[I](sQuery, tQuery, partitioner, nonSTQuery)
  //  }
}
