package operators.extractor

import instances.{Duration, Event, Geometry, Instance, Point, Polygon, RTree, Trajectory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

object Join {
  private def buildRTree[S <: Geometry : ClassTag](geomArr: Array[S], durArr: Array[Duration]): RTree[S] = {
    val r = math.sqrt(geomArr.length).toInt
    var entries = new Array[(S, String, Int)](0)
    for (i <- geomArr.indices) {
      geomArr(i).setUserData(Array(durArr(i).start.toDouble, durArr(i).end.toDouble))
      entries = entries :+ (geomArr(i).copy.asInstanceOf[S], (geomArr(i)).hashCode().toString, i)
    }
    RTree[S](entries, r, dimension = 3)
  }

  implicit class BcJoin[T1 <: Instance[_, _, _] : ClassTag](rdd: RDD[T1]) extends Serializable {
    def bcJoin[T2 <: Instance[_, _, _] : ClassTag](bcRdd: RDD[T2]): RDD[(T1, T2)] = {
      val spark = SparkSession.builder().getOrCreate()
      val bc = spark.sparkContext.broadcast(bcRdd.collect)
      rdd.flatMap {
        x => bc.value.filter(_.intersects(x.toGeometry, x.duration)).map(y => (x, y))
      }
    }

    def bcJoinRTree[T2 <: Instance[_, _, _] : ClassTag](bcRdd: RDD[T2]): RDD[(T2, Geometry)] = {
      val spark = SparkSession.builder().getOrCreate()
      val bc = spark.sparkContext.broadcast(bcRdd.collect)
      rdd.mapPartitions(p => {
        val instances = p.toArray
        val rTree = buildRTree(instances.map(_.toGeometry), instances.map(_.duration))
       println(instances.deep)
       println(bc.value.deep)
       println(bc.value.map(x => rTree.range3d(x)).deep)
        bc.value.flatMap(x => rTree.range3d(x).map(y => (x, y._1))).toIterator
      })
    }
  }
}
