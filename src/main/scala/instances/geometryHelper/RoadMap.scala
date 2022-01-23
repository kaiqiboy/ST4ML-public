package instances.geometryHelper

import scala.reflect.ClassTag

case class RoadMap[T: ClassTag](roadID: String, attributes: T) {

}
