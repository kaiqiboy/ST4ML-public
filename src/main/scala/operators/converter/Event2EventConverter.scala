package operators.converter

import instances.GeometryImplicits._
import instances._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

class Event2EventConverter[V: ClassTag, D: ClassTag](map: Array[LineString],
                                                     dist: Double = 500) extends Converter {
  type I = Event[Point, V, D]
  type O = Event[Point, V, String]
  override val optimization: String = ""
  val entries: Array[(LineString, String, Int)] = map.zipWithIndex.map(x => (x._1, x._2.toString, x._2))
  val rTree: RTree[LineString] = RTree(entries, scala.math.sqrt(entries.length).toInt)

  def convert(input: RDD[I], discard: Boolean = false): RDD[O] = {
    val resRDD = input.map(event => {
      val candidates = rTree.distanceRange(event.toGeometry, dist)
      val id = if (!candidates.isEmpty) candidates.map(x => (x._1.distance(event.toGeometry), x._2.toInt))
        .minBy(_._1)._2
      else -1
      if (id == -1) Event(event.entries, "not-matched")
      else {
        val projected = event.spatialCenter.project(map(id))._1
        Event(projected, event.entries.head.temporal, event.entries.head.value, event.data.toString)
      }
    })
    if (discard) resRDD.filter(_.data != "not-matched") else resRDD
  }
}
