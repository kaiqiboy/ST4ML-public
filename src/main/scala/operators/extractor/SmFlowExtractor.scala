package operators.extractor

import instances.{Geometry, Polygon, SpatialMap, Trajectory}
import org.apache.spark.rdd.RDD
import instances.Utils._
import scala.reflect.ClassTag

class SmFlowExtractor[S <: Geometry : ClassTag, D: ClassTag, T <: SpatialMap[S, Array[Trajectory[_, _]], D] : ClassTag] extends Extractor[T] {
  def extract(smRDD: RDD[T]): SpatialMap[S, Int, None.type] = {
    val flowRDD = smRDD.map(_.mapValue(_.length).mapData(_ => None))

    def plus(x: Int, y: Int): Int = x + y

    flowRDD.collectAndMerge(0, plus)
  }
}