package instances

object onDiskFormats {

  /** case classes for persisting ST instances */

  case class STEvent(shape: String, timeStamp: Array[Long], v: Option[String], d: String)

  case class STEventwP(shape: String, timeStamp: Array[Long], v: Option[String], d: String, pId: Int) // with partition id

  case class TrajPoint(lon: Double, lat: Double, t: Array[Long], v: Option[String])

  case class STTraj(points: Array[TrajPoint], d: String)

  case class STTrajwP(points: Array[TrajPoint], d: String, pId: Int)

  case class RCell(shape: String, temporal: String, id: Option[String] = None)

  case class STRaster(cells: Array[RCell], id: Option[String] = None)

  case class SCell(shape: String, id: Option[String] = None)

  case class STSm(cells: Array[SCell], id: Option[String] = None)

  case class TCell(temporal: String, id: Option[String] = None)

  case class STTs(cells: Array[TCell], id: Option[String] = None)

  /**
   * The shape string follows JTS format, e.g.:
   * POINT (0 0)
   * POLYGON ((0 1, 0 3, 2 3, 2 1, 0 1))
   * LINESTRING (0 0, 1 1)
   *
   * The temporal string follows the format
   * Duration (0, 1)
   */
}
