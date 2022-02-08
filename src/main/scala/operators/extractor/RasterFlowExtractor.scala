package operators.extractor

import instances.Utils._
import instances.{Geometry, Raster, Trajectory}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class RasterFlowExtractor[S <: Geometry : ClassTag, D: ClassTag, T <: Raster[S, Array[Trajectory[_, _]], D] : ClassTag] extends Extractor[T] {
  def extract(smRDD: RDD[T]): Raster[S, Int, None.type] = {
    val flowRDD = smRDD.map(_.mapValue(_.length).mapData(_ => None))

    def plus(x: Int, y: Int): Int = x + y

    flowRDD.collectAndMerge(0, plus)
  }
}