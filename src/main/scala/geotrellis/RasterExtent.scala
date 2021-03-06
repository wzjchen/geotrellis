package geotrellis

import scala.math.{min, max, round, ceil, floor}

case class GeoAttrsError(msg:String) extends Exception(msg)

/**
 * Represents grid coordinates of a subsection of a RasterExtent.
 * These coordinates are inclusive.
 */
case class GridBounds(colMin:Int,rowMin:Int,colMax:Int,rowMax:Int) {
  val width = colMax - colMin + 1
  val height = rowMax - rowMin + 1
}

object GridBounds {
  def apply(r:Raster):GridBounds = 
    GridBounds(0,0,r.rasterExtent.cols,r.rasterExtent.rows)
}

/**
 * RasterExtent objects represent the geographic extent (envelope) of a raster.
 *
 * The Raster extent has two coordinate concepts involved: map coordinates and grid
 * coordinates. Map coordinates are what the [[Extent]] class uses, and specifies points
 * using an X coordinate and a Y coordinate. The X coordinate is oriented along west to east
 * such that the larger the X coordinate, the more eastern the point. The Y coordinate is
 * along south to north such that the larger the Y coordinate, the more Northern the point.
 *
 * This contrasts with the grid coordinate system. The grid coordinate system does not
 * actually reference points on the map, but instead a cell of the raster that represents
 * values for some square area of the map. The column axis is similar in that the number
 * gets larger as one goes from west to east; however, the row axis is inverted from map coordinates:
 * as the row number increases, the cell is heading south. The top row is labeled as 0, and the next
 * 1, so that the highest indexed row is the southern most row of the raster.
 * A cell has a height and a width that is in terms of map units. You can think of it as each cell
 * is itself an extent, with width cellwidth and height cellheight. When a cell needs
 * to be represented or thought of as a point, the center of the cell will be used.
 * So when gridToMap is called, what is returned is the center point, in map coordinates.
 *
 * Map points are considered to be 'inside' the cell based on these rules:
 *  - If the point is inside the area of the cell, it is included in the cell.
 *  - If the point lies on the north or west border of the cell, it is included in the cell.
 *  - If the point lies on the south or east border of the cell, it is not included in the cell,
 *    it is included in the next southern or eastern cell, respectively.
 *
 * Note that based on these rules, the eastern and southern borders of an Extent are not actually
 * considered to be part of the RasterExtent.
 */
case class RasterExtent(extent:Extent, cellwidth:Double, cellheight:Double, cols:Int, rows:Int) {

  if (cellwidth  <= 0.0) throw GeoAttrsError("invalid cell-width")
  if (cellheight <= 0.0) throw GeoAttrsError("invalid cell-height")
  if (cols <= 0) throw GeoAttrsError("invalid cols")
  if (rows <= 0) throw GeoAttrsError("invalid rows")

  val width  = extent.width
  val height = extent.height

  /**
   * The size of the extent, e.g. cols * rows.
   */
  def size = cols * rows

  /**
   * Compare this object with another GeoAttrs object, as per the comparison
   * rules in Extent#compare.
   */
  def compare(other:RasterExtent) = extent.compare(other.extent)

  /**
   * Determine if the underlying extent contains the given point.
   */
  def containsPoint(x:Double, y:Double) = extent.containsPoint(x, y)
  
  /**
   * Convert map coordinates (x,y) to grid coordinates (col,row).
   */
  def mapToGrid(x:Double, y:Double) = {
    val col = ((x - extent.xmin) / cellwidth).toInt
    val row = ((extent.ymax - y) / cellheight).toInt
    (col, row)
  }

  /**
   * Convert map coordinate x to grid coordinate column.
   */
  def mapXToGrid(x:Double) = mapXToGridDouble(x).toInt
  def mapXToGridDouble(x:Double) = (x - extent.xmin) / cellwidth
    
  /**
   * Convert map coordinate y to grid coordinate row.
   */
  def mapYToGrid(y:Double) = mapYToGridDouble(y).toInt
  def mapYToGridDouble(y:Double) = (extent.ymax - y ) / cellheight
  
  /**
   * Convert map coordinate tuple (x,y) to grid coordinates (col,row).
   */
  def mapToGrid(mapCoord:(Double,Double)):(Int,Int) = {
    val (x,y) = mapCoord;
    mapToGrid(x,y)
  }

  /**
    * The map coordinate of a grid cell is the center point.
    */  
  def gridToMap(col:Int, row:Int) = {
    val x = max(min(col * cellwidth + extent.xmin + (cellwidth / 2), extent.xmax), extent.xmin)
    val y = min(max(extent.ymax - (row * cellheight) - (cellheight / 2), extent.ymin), extent.ymax)
    (x, y)
  }

  def gridColToMap(col:Int) = {
    max(min(col * cellwidth + extent.xmin + (cellwidth / 2), extent.xmax), extent.xmin)
  }

  def gridRowToMap(row:Int) = {
    min(max(extent.ymax - (row * cellheight) - (cellheight / 2), extent.ymin), extent.ymax)
  }

  /**
   * Gets the GridBounds for this RasterExtent that is the smallest subgrid
   * containing all points within the extent. The extent is considered inclusive
   * on it's north and west borders, exclusive on it's east and south borders.
   * See [[RasterExtent]] for a discussion of grid and extent boundary concepts.
   */
  def gridBoundsFor(subExtent:Extent):GridBounds = {
    // West and North boundarys are a simple mapToGrid call.
    val (colMin,rowMin) = mapToGrid(subExtent.xmin, subExtent.ymax)

    // If South East corner is on grid border lines, we want to still only include
    // what is to the West and\or North of the point. However if the border point
    // is not directly on a grid division, include the whole row and/or column that
    // contains the point.
    val colMax = ceil((subExtent.xmax - extent.xmin) / cellwidth).toInt - 1
    val rowMax = ceil((extent.ymax - subExtent.ymin) / cellheight).toInt - 1
    
    GridBounds(colMin,
               rowMin,
               colMax,
               rowMax)
  }
  
  /**
   * Combine two different RasterExtents (which must have the same cellsizes).
   * The result is a new extent at the same resolution.
   */
  def combine (that:RasterExtent):RasterExtent = {
    if (cellwidth != that.cellwidth)
      throw GeoAttrsError("illegal cellwidths: %s and %s".format(cellwidth, that.cellwidth))
    if (cellheight != that.cellheight)
      throw GeoAttrsError("illegal cellheights: %s and %s".format(cellheight, that.cellheight))

    val newExtent = extent.combine(that.extent)
    val newRows = ceil(newExtent.height / cellheight).toInt
    val newCols = ceil(newExtent.width / cellwidth).toInt

    RasterExtent(newExtent, cellwidth, cellheight, newCols, newRows)
  }

  /**
   * Returns a RasterExtent with the same extent,
   * but a modified number of columns and rows based
   * on the given cell height and width.
   */
  def withResolution(targetCellWidth:Double,targetCellHeight:Double):RasterExtent = {
    val newCols = math.ceil((extent.xmax - extent.xmin) / targetCellWidth).toInt
    val newRows = math.ceil((extent.ymax - extent.ymin) / targetCellHeight).toInt
    RasterExtent(extent,targetCellWidth,targetCellHeight,newCols,newRows)
  }

  /**
   * Returns a RasterExtent that lines up with this RasterExtent's resolution,
   * and grid layout.
   * i.e., the resulting RasterExtent will not have the given extent,
   * but will have the smallest extent such that the whole of 
   * the given extent is covered, that lines up with the grid.
   */
  def createAligned(targetExtent:Extent):RasterExtent = {
    val xmin = extent.xmin + (math.floor((targetExtent.xmin - extent.xmin) / cellwidth) * cellwidth)
    val xmax = extent.xmax - (math.floor((extent.xmax - targetExtent.xmax) / cellwidth) * cellwidth)
    val ymin = extent.ymin + (math.floor((targetExtent.ymin - extent.ymin) / cellheight) * cellheight)
    val ymax = extent.ymax - (math.floor((extent.ymax - targetExtent.ymax) / cellheight) * cellheight)

    val targetCols = math.round((xmax - xmin) / cellwidth).toInt
    val targetRows = math.round((ymax - ymin) / cellheight).toInt
    RasterExtent(Extent(xmin,ymin,xmax,ymax),cellwidth,cellheight,targetCols,targetRows)
  }

  def extentFor(gridBounds:GridBounds):Extent = {
    val xmin = max(min(gridBounds.colMin * cellwidth + extent.xmin, extent.xmax) , extent.xmin)
    val ymax = min(max(extent.ymax - (gridBounds.rowMin * cellheight), extent.ymin), extent.ymax)
    val xmax = xmin + (gridBounds.width * cellwidth)
    val ymin = ymax - (gridBounds.height * cellheight)
    Extent(xmin,ymin,xmax,ymax)
  }
}

object RasterExtent {
  def apply(extent:Extent, cols:Int, rows:Int):RasterExtent = {
    val cw = (extent.xmax - extent.xmin) / cols
    val ch = (extent.ymax - extent.ymin) / rows
    RasterExtent(extent, cw, ch, cols, rows)
  }

  def apply(extent:Extent, cellwidth:Double, cellheight:Double):RasterExtent = {
    val cols = ((extent.ymax - extent.ymin) / cellheight).toInt
    val rows = ((extent.xmax - extent.xmin) / cellwidth).toInt
    RasterExtent(extent, cellwidth, cellheight, cols, rows)
  }
}
