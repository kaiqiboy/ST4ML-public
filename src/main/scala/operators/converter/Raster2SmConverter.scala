package operators.converter

import instances.{Duration, Entry, Extent, Geometry, Polygon, Raster, SpatialMap, TimeSeries}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

// only fully contained cells are converted
class Raster2SmConverter[S <: Geometry : ClassTag,
  V: ClassTag, D: ClassTag, V2: ClassTag](sArray: Array[Polygon]) extends Converter {
  type I = Raster[S, V, D]
  type O = SpatialMap[Polygon, V2, None.type]
  override val optimization: String = ""

  def convert(input: RDD[I], f: Array[V] => V2): RDD[O] = {
    input.map { raster =>
      val grouped = raster.entries.map(entry =>
        (sArray.zipWithIndex.find(_._1.contains(entry.spatial)), entry))
        .filter(_._1.isDefined)
        .map(x => (x._1.get, x._2.value))
        .groupBy(_._1).map(x => (x._1, f(x._2.map(_._2)))).toArray
      new SpatialMap[Polygon, V2, None.type](grouped.map(x =>
        new Entry(x._1._1, Duration.empty, x._2)), None)
    }
  }
}