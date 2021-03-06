package geotrellis.raster

import geotrellis._

/**
 * RasterData based on Array[Byte] (each cell as a Byte).
 */
final case class ByteArrayRasterData(array: Array[Byte], cols: Int, rows: Int)
  extends MutableRasterData with IntBasedArray {
  def getType = TypeByte
  def alloc(cols: Int, rows: Int) = ByteArrayRasterData.ofDim(cols, rows)
  def length = array.length
  def apply(i: Int) = b2i(array(i))
  def update(i: Int, z: Int) { array(i) = i2b(z) }
  def copy = ByteArrayRasterData(array.clone, cols, rows)
}

object ByteArrayRasterData {
  def ofDim(cols: Int, rows: Int) = new ByteArrayRasterData(Array.ofDim[Byte](cols * rows), cols, rows)
  def empty(cols: Int, rows: Int) = new ByteArrayRasterData(Array.fill[Byte](cols * rows)(Byte.MinValue), cols, rows)
}

