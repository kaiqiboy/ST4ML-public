package operators.converter

import instances.geometryHelper.road.{RoadGraph, RoadGrid}
import instances.GeometryImplicits.withExtraPointOps
import instances.RoadNetwork.RoadNetwork
import instances._
import instances.onDiskFormats.STTraj
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.Coordinate

import scala.language.implicitConversions
import scala.math._
import scala.reflect.ClassTag


class MapMatcher(fileDir: String) extends Serializable {

  case class RoadSeg(id: String, shape: LineString)


  val roadGrid: RoadGrid = loadRoadGraph(fileDir)
  val roadNetwork: RoadNetwork = genRoadNetwork(roadGrid)
  val roadGraph: RoadGraph = RoadGraph(roadGrid.edges)

  // read osm files to generate a spatial map of road networks
  def loadRoadGraph(fileDir: String): RoadGrid = RoadGrid(fileDir)

  // generate the companion road network for candidate searching
  def genRoadNetwork(rg: RoadGrid): RoadNetwork = {
    val edges = rg.edges
    val edgeCoordinates = edges.map(_.ls.coordinates)
    val lineStrings = edgeCoordinates.map(edge => LineString(edge.map(c => new Coordinate(c(0), c(1)))))
    val entries = (edges zip lineStrings).map {
      case (edge, lineString) => new Entry(lineString, Duration.empty, edge.id)
    }
    new SpatialMap[LineString, String, None.type](entries, None)
  }

  // build rTree for lineString spatial map
  def buildRTree(spatials: Array[LineString]): RTree[LineString] = {
    val r = math.sqrt(spatials.length).toInt
    val entries = spatials.zipWithIndex.map(x => (x._1, x._2.toString, x._2))
    RTree[LineString](entries, r)
  }

  // find candidates of each point in a trajectory in a road network with a threshold using RTree
  // distance calculation is not accurate
  def findCandidate[T <: Trajectory[_, _] : ClassTag](traj: T,
                                                      threshold: Double): Array[(Point, Array[RoadSeg])] = {
    val pArray = traj.entries.map(_.spatial)
    roadNetwork.rTree = Some(buildRTree(roadNetwork.spatials))
    val candidates = roadNetwork.getSpatialIndexRTree(pArray, threshold)
    val roadSegs = roadNetwork.entries.map(x => RoadSeg(x.value, x.spatial))
    candidates.map(x => x.map(i => roadSegs(i)))
    pArray zip candidates.map(x => x.map(i => roadSegs(i)))
  }

  //    // find candidates of each point in a trajectory in a road network with a threshold
  //    def findCandidate[T <: Trajectory[_, _] : ClassTag](traj: T,
  //                                                        threshold: Double): Array[(Point, Array[RoadSeg])] = {
  //      val pArray = traj.entries.map(_.spatial)
  //      val roadSegs = roadNetwork.entries.map(x => RoadSeg(x.value, x.spatial))
  //      pArray.map(x => (x, roadSegs.filter(rs => x.greatCircle(rs.shape) <= threshold)))
  //    }

  // calculate emission probability
  def calEmissionProb(p: Point, l: LineString, sigmaZ: Double): Double = {
    val d = p.greatCircle(l)
    1 / (sqrt(2 * Pi) * sigmaZ) * pow(E, -0.5 * pow(d / sigmaZ, 2))
  }

  // dijkstra shortest path, input road segment Ids and return distance in meter
  // id1 and id2 are different
  def findShortestPath(x: Point, y: Point, id1: String, id2: String): Double = {
    val startVertex = id1.split("-")(1) // to vertex of the src edge
    val endVertex = id2.split("-")(0) // from vertex of the dst edge
    val p0 = roadGrid.id2vertex(startVertex).point
    val p1 = roadGrid.id2vertex(endVertex).point
    val edges = roadGrid.getGraphEdgesByPoint(p0, p1)
    val rGraph = RoadGraph(edges)
    rGraph.getShortestPathAndLength(startVertex, endVertex)._2 +
      p0.geoDistance(geometryHelper.Point(Array(x.x, x.y))) + p1.geoDistance(geometryHelper.Point(Array(y.x, y.y)))
  }

  //find transition probability of consecutive points
  def calTransitionProb(p1: Point, p2: Point, road1: RoadSeg, road2: RoadSeg, beta: Double): Double = {
    if (road1.id == road2.id) 1 / beta * pow(E, 0)
    else {
      val roadDist = findShortestPath(p1: Point, p2: Point, road1.id, road2.id)
      val dt = abs(p1.greatCircle(p2) - roadDist)
      if (dt > 2000) 0
      else 1 / beta * pow(E, -dt / beta)
    }
  }

  // calculate the emission matrix, using RoadNetwork (spatial map with rTree)
  def genEmissionMatrix[T <: Trajectory[_, _] : ClassTag](candidates: Array[(Point, Array[RoadSeg])],
                                                          sigmaZ: Double = 4.07): Array[Array[Double]] = {
    candidates.map { case (p, candidateArr) =>
      candidateArr.map(x => calEmissionProb(p, x.shape, sigmaZ))
    }
  }

  // calculate the transition matrix
  def genTransitionMatrix(candidates: Array[(Point, Array[RoadSeg])], beta: Double = 0.2): Array[Array[Array[Double]]] = {
    candidates.sliding(2).map { x =>
      val o = x(0)._2.map(r => (x(0)._1, r))
      val d = x(1)._2.map(r => (x(1)._1, r))
      o.map(x => d.map(y => calTransitionProb(x._1, y._1, x._2, y._2, beta)))
    }.toArray
  }

  // viterbi algo to find the best path, return a list of idx of each candidate group
  def viterbi(eProbs: Array[Array[Double]], tProbs: Array[Array[Array[Double]]]): Array[Int] = {
    if (!eProbs.map(_.length).forall(_ > 0)) Array(-1)
    else {
      val states = Array.ofDim[List[Int]](eProbs.length, eProbs.map(_.length).max)
      val probs = Array.ofDim[Double](eProbs.length, eProbs.map(_.length).max)
      eProbs(0).indices.foreach(i => states(0)(i) = List(i))
      probs(0) = eProbs(0)
      (1 until eProbs.length).foreach { t =>
        eProbs(t).indices.foreach { c =>
          var candiProbs = new Array[Double](0)
          eProbs(t - 1).indices.foreach { last =>
            val newProb = probs(t - 1)(last) * tProbs(t - 1)(last)(c) * eProbs(t)(c)
            candiProbs = candiProbs :+ newProb
          }
          val index = max(0, candiProbs.indexOf(candiProbs.max))
          probs(t)(c) = candiProbs(index)
          states(t)(c) = c :: states(t - 1)(index)
        }
      }
      val finalProb = probs(eProbs.length - 1)
      val maxLastCandidate = finalProb.indexOf(finalProb.max)
      states(eProbs.length - 1)(maxLastCandidate).reverse.toArray
    }
  }


  // infer timestamps for the interpolated points based on speed information, not accurate and time-consuming
  def connectRoadsInfer(idx: Array[Int], candidates: Array[(Point, Array[RoadSeg])], timeStamps: Array[Long]): Array[Entry[Point, String]] = {
    val segNPoint = candidates.zip(idx).map(x => (x._1._2(x._2), x._1._1)).zip(timeStamps).map(x => (x._1._1, x._1._2, x._2))
    var segNPointGrouped = new Array[(RoadSeg, Array[(Point, Long)])](0)
    for (i <- segNPoint) {
      if (segNPointGrouped.length == 0 || segNPointGrouped.last._1 != i._1) segNPointGrouped = segNPointGrouped :+ (i._1, Array((i._2, i._3)))
      else {
        val last = segNPointGrouped.last
        segNPointGrouped = segNPointGrouped.dropRight(1) :+ (last._1, last._2 :+ (i._2, i._3))
      }
    }
    val interpolated = segNPointGrouped.sliding(2).map { segs =>
      val start = segs(0)
      val end = segs(1)
      if (start._1.id.split("-").last != end._1.id.split("-").head) {
        try {
          val (internal, totalDist) = roadGraph.getShortestPathAndLength(start._1.id, end._1.id)
          val tEnd = end._2.last._2
          val tStart = start._2.head._2
          val edges = internal.sliding(2).map(x => roadGrid.id2edge(x.head + "-" + x(1))).toArray.drop(1).dropRight(1)
          edges.map(edge => {
            val startPoint = Point(edge.coordinates.head, edge.coordinates(1))
            val endPointOfLastSegId = start._1.id.split("-").last
            val endPointOfLastSeg = roadGrid.id2vertex(endPointOfLastSegId).point
            val dist = roadGraph.getShortestPathAndLength(edge.id.split("-").head, endPointOfLastSegId)._2 +
              startPoint.greatCircle(Point(endPointOfLastSeg.x, endPointOfLastSeg.y))
            val ratio = dist / totalDist
            val t = (tStart + (tEnd - tStart) * ratio).toLong
            Entry(startPoint, Duration(t), edge.id)
          })
        }
        catch {
          case _: Any => new Array[Entry[Point, String]](0)
        }
      }
      else new Array[Entry[Point, String]](0)
    }.toArray :+ new Array[Entry[Point, String]](0)
    val segNProjectedPoints = segNPointGrouped.map(x => (x._1, x._2.map { case (p, t) =>
      val (_, projected) = roadGrid.id2edge(x._1.id).ls.projectionDistance(geometryHelper.Point(Array(p.x, p.y)))
      new Entry(Point(projected.x, projected.y), Duration(t), x._1.id)
    }))

    implicit def flatTup[T](t: (T, T)): List[T] = t match {
      case (a, b) => List(a, b)
    }

    (segNProjectedPoints.map(_._2) zip interpolated).flatten.flatten
  }

  // record the temporal range for inferred points
  def connectRoads(idx: Array[Int], candidates: Array[(Point, Array[RoadSeg])], timeStamps: Array[Long]): Array[Entry[Point, String]] = {
    val segNPoint = candidates.zip(idx).map(x => (x._1._2(x._2), x._1._1)).zip(timeStamps).map(x => (x._1._1, x._1._2, x._2))
    var segNPointGrouped = new Array[(RoadSeg, Array[(Point, Long)])](0)
    for (i <- segNPoint) {
      if (segNPointGrouped.length == 0 || segNPointGrouped.last._1 != i._1) segNPointGrouped = segNPointGrouped :+ (i._1, Array((i._2, i._3)))
      else {
        val last = segNPointGrouped.last
        segNPointGrouped = segNPointGrouped.dropRight(1) :+ (last._1, last._2 :+ (i._2, i._3))
      }
    }
    if (segNPointGrouped.length > 1) {
      val interpolated = segNPointGrouped.sliding(2).map { segs =>
        val start = segs(0)
        val end = segs(1)
        if (start._1.id.split("-").last != end._1.id.split("-").head) {
          try {
            val (internal, totalDist) = roadGraph.getShortestPathAndLength(start._1.id, end._1.id)
            val tEnd = end._2.last._2
            val tStart = start._2.head._2
            val edges = internal.sliding(2).map(x => roadGrid.id2edge(x.head + "-" + x(1))).toArray.drop(1).dropRight(1)
            edges.map(edge => {
              val startPoint = Point(edge.coordinates.head, edge.coordinates(1))
              Entry(startPoint, Duration(tStart, tEnd), edge.id)
            })
          }
          catch {
            case _: Any => new Array[Entry[Point, String]](0)
          }
        }
        else new Array[Entry[Point, String]](0)
      }.toArray :+ new Array[Entry[Point, String]](0)
      val segNProjectedPoints = segNPointGrouped.map(x => (x._1, x._2.map { case (p, t) =>
        val (_, projected) = roadGrid.id2edge(x._1.id).ls.projectionDistance(geometryHelper.Point(Array(p.x, p.y)))
        new Entry(Point(projected.x, projected.y), Duration(t), x._1.id)
      }))

      implicit def flatTup[T](t: (T, T)): List[T] = t match {
        case (a, b) => List(a, b)
      }

      (segNProjectedPoints.map(_._2) zip interpolated).flatten.flatten
    } else {
      val segNProjectedPoints = segNPointGrouped.map(x => (x._1, x._2.map { case (p, t) =>
        val (_, projected) = roadGrid.id2edge(x._1.id).ls.projectionDistance(geometryHelper.Point(Array(p.x, p.y)))
        new Entry(Point(projected.x, projected.y), Duration(t), x._1.id)
      }))
      segNProjectedPoints.flatMap(_._2)
    }
  }

  def mapPoints(idx: Array[Int], candidates: Array[(Point, Array[RoadSeg])], timestamps: Array[Long]): Array[Entry[Point, String]] = {
    val selectedPoints = (candidates zip idx).map {
      case (candidate, id) => (candidate._1, candidate._2(id))
    }
    val entries = (selectedPoints zip timestamps).map {
      case ((point, roadSeg), t) =>
        val id = roadSeg.id
        val (_, projected) = roadGrid.id2edge(id).ls.projectionDistance(geometryHelper.Point(Array(point.x, point.y)))
        Entry(Point(projected.x, projected.y), Duration(t), id)
    }
    entries
  }

  def mapMatchWithInterpolation[T <: Trajectory[_, _] : ClassTag](traj: T, candidateThresh: Double = 50,
                                                                  sigmaZ: Double = 4.07, beta: Double = 0.2, inferTime: Boolean = false): Trajectory[String, String] = {
    val candidates = findCandidate[T](traj, candidateThresh)
    val eMatrix = genEmissionMatrix(candidates, sigmaZ)
    val cleanedEMatrix = eMatrix.zipWithIndex.filter(x => x._1.length > 0 && (!x._1.forall(_ <= 0))) // remove points with all 0 emission probs or no candidates
    val validPoints = cleanedEMatrix.map(_._2)
    val cleanedCandidates = candidates.zipWithIndex.filter(x => validPoints.contains(x._2)).map(_._1)
    val cleanedTimeStamps = traj.entries.map(_.temporal.start).zipWithIndex.filter(x => validPoints.contains(x._2)).map(_._1)
    val opimalPathIdx = if (cleanedCandidates.length < 2) Array(-1)
    else {
      val tMatrix = genTransitionMatrix(cleanedCandidates, beta)
      viterbi(cleanedEMatrix.map(_._1), tMatrix)
    }
    if (opimalPathIdx sameElements Array(-1)) return new Trajectory(Array(Entry(Point.empty, Duration.empty, ""), Entry(Point.empty, Duration.empty, "")), "invalid")
    val connected = if (inferTime) connectRoadsInfer(opimalPathIdx, cleanedCandidates, cleanedTimeStamps) else
      connectRoads(opimalPathIdx, cleanedCandidates, cleanedTimeStamps)
    Trajectory(connected, traj.data.toString)
  }

  def mapMatch[T <: Trajectory[_, _] : ClassTag](traj: T, candidateThresh: Double = 50,
                                                              sigmaZ: Double = 4.07, beta: Double = 0.2): Trajectory[String, String] = {
    val candidates = findCandidate[T](traj, candidateThresh)
    val eMatrix = genEmissionMatrix(candidates, sigmaZ)
    val cleanedEMatrix = eMatrix.zipWithIndex.filter(x => x._1.length > 0 && (!x._1.forall(_ <= 0))) // remove points with all 0 emission probs or no candidates
    val validPoints = cleanedEMatrix.map(_._2)
    val cleanedCandidates = candidates.zipWithIndex.filter(x => validPoints.contains(x._2)).map(_._1)
    val cleanedTimeStamps = traj.entries.map(_.temporal.start).zipWithIndex.filter(x => validPoints.contains(x._2)).map(_._1)
    val opimalPathIdx = if (cleanedCandidates.length < 2) Array(-1)
    else {
      val tMatrix = genTransitionMatrix(cleanedCandidates, beta)
      viterbi(cleanedEMatrix.map(_._1), tMatrix)
    }
    if (opimalPathIdx sameElements Array(-1))
      return new Trajectory(Array(Entry(Point.empty, Duration.empty, ""), Entry(Point.empty, Duration.empty, "")), "invalid")
    else {
      val entries = mapPoints(opimalPathIdx, cleanedCandidates, cleanedTimeStamps)
      new Trajectory[String, String](entries, traj.data.toString)
    }
  }
}
