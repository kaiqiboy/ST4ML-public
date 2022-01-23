package operators.converter

import instances.{Duration, Entry, Event, Point, Trajectory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import operators.selector.SelectionUtils._
import org.apache.spark.HashPartitioner

import java.lang.System.nanoTime
import scala.reflect.ClassTag

class Event2TrajConverter extends Converter {
  override val optimization = "none"

  def convert[EV: ClassTag, ED: ClassTag](input: RDD[Event[Point, EV, ED]]): RDD[Trajectory[EV, ED]] = {
    type I = Event[Point, EV, ED]
    type O = Trajectory[EV, ED]
    input.map(e => (e.data, e.entries))
      .reduceByKey((x, y) => x ++ y)
      .map(x => {
        val entries = x._2.sortBy(_.temporal.start)
        new Trajectory(entries, x._1)
      })
  }

  def convert[EV: ClassTag, ED: ClassTag](input: RDD[Event[Point, EV, ED]], numPartitions: Int): RDD[Trajectory[EV, ED]] = {
    type I = Event[Point, EV, ED]
    type O = Trajectory[EV, ED]
    input.map(e => (e.data, e.entries))
      .reduceByKey((x, y) => x ++ y, numPartitions)
      .map(x => {
        val entries = x._2.sortBy(_.temporal.start)
        new Trajectory(entries, x._1)
      })
  }
}


