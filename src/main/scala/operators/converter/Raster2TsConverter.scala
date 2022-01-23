package operators.converter

import instances.{Duration, Entry, Extent, Geometry, Polygon, Raster, TimeSeries}
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

class Raster2TsConverter[S <: Geometry : ClassTag,
  V: ClassTag, D: ClassTag, V2: ClassTag](tArray: Array[Duration]) extends Converter {
  type I = Raster[S, V, D]
  type O = TimeSeries[V2, None.type]
  override val optimization: String = ""

  def convert(input: RDD[I], f: Array[V] => V2): RDD[O] = {
    input.map { raster =>
      val grouped = raster.entries.map(entry =>
        (tArray.zipWithIndex.find(_._1.contains(entry.temporal)), entry))
        .filter(_._1.isDefined)
        .map(x => (x._1.get, x._2.value))
        .groupBy(_._1).map(x => (x._1, f(x._2.map(_._2)))).toArray
      new TimeSeries[V2, None.type](grouped.map(x =>
        new Entry(Polygon.empty, x._1._1, x._2)), None)
    }
  }
}
