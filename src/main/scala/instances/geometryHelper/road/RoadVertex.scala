package instances.geometryHelper.road

import instances.geometryHelper.Point

final case class RoadVertex(id: String, point: Point) {
  def geoDistance(other: Point): Double = this.point.geoDistance(other)
}