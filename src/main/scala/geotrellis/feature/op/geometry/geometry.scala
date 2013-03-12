package geotrellis.feature.op

import geotrellis._
import geotrellis.feature._
import geotrellis.{ op => liftOp }
import com.vividsolutions.jts.{ geom => jts }


package object geometry {

  val GetExtent = liftOp { (a: Raster) => a.rasterExtent.extent }
  val AsFeature = liftOp { (e: Extent) => e.asFeature(()) }

  /**
   * Given a Geometry object, inspect the underlying geometry type
   * and recursively flatten it if it is a GeometryCollection
   */
  case class FlattenGeometry[D](g1: Op[Geometry[D]]) extends Operation[List[Geometry[D]]] {

    def flattenGeometry(g: jts.Geometry):List[jts.Geometry] = g match {
        case g: jts.GeometryCollection => (0 until g.getNumGeometries).flatMap(
          i => flattenGeometry(g.getGeometryN(i))).toList
        case l: jts.LineString => List(l)
        case p: jts.Point => List(p)
        case p: jts.Polygon => List(p)
      }

    def _run(context:Context) = runAsync(List(g1))
    val nextSteps:Steps = {
      case a :: Nil => {
        val g = a.asInstanceOf[Geometry[D]]
        val geoms = flattenGeometry(g.geom)
        Result(geoms.map(geom => Feature(geom, g.data)))
      }
    }
  }


  /**
   * Returns a Geometry as a Polygon Set.
   */
  case class AsPolygonSet[D](g: Op[Geometry[D]]) extends Operation[List[Polygon[D]]] {    
    //val compositeOp = FilterGeometry[jts.Polygon,D](FlattenGeometry(g))

    def _run(context:Context) = StepError("Not implemented in 2.9.2 branch", "")
    val nextSteps:Steps = {
      case a :: Nil => Result(a.asInstanceOf[List[Polygon[D]]])
    }
  }
}
