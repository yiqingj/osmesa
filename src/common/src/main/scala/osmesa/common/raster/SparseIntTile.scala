package osmesa.common.raster

import geotrellis.raster.{
  ArrayTile,
  CellType,
  IntCellType,
  IntCells,
  IntConstantNoDataCellType,
  IntTileVisitor,
  IntUserDefinedNoDataCellType,
  MutableArrayTile,
  NoDataHandling,
  Tile
}

class SparseIntTile(val cols: Int,
                    val rows: Int,
                    val values: Map[Long, Int],
                    val cellType: IntCells with NoDataHandling)
    extends ArrayTile {
  private val noDataValue = cellType match {
    case IntConstantNoDataCellType        => Int.MinValue
    case IntUserDefinedNoDataCellType(nd) => nd
    case IntCellType                      => 0
  }

  def interpretAs(newCellType: CellType): Tile = {
    newCellType match {
      case dt: IntCells with NoDataHandling =>
        SparseIntTile(cols, rows, values, dt)
      case _ =>
        withNoData(None).convert(newCellType)
    }
  }

  def withNoData(noDataValue: Option[Double]): Tile =
    SparseIntTile(cols, rows, values, cellType.withNoData(noDataValue))

  override def applyDouble(i: Int): Double = apply(i).toDouble

  override def apply(i: Int): Int = values.getOrElse(i, noDataValue)

  override def copy: ArrayTile = SparseIntTile(cols, rows, Map(values.toSeq: _*), cellType)

  // unimplemented because it doesn't make sense in this context (and SparseIntTile can't be instantiated from
  // Array[Byte])
  override def toBytes(): Array[Byte] = ???

  def toMap: Map[Long, Int] = values

  override def mutable: MutableArrayTile =
    MutableSparseIntTile(cols, rows, scala.collection.mutable.LongMap(values.toSeq: _*), cellType)

  override def foreachIntVisitor(visitor: IntTileVisitor): Unit = {
    // NOTE only visits coordinates containing data; this isn't strictly correct for some uses
    values.foreach {
      case (k, v) =>
        val col = k % cols
        val row = k / cols

        visitor(col.toInt, row.toInt, v)
    }
  }
}

object SparseIntTile {
  def apply(cols: Int,
            rows: Int,
            values: Map[Long, Int] = Map.empty[Long, Int],
            cellType: IntCells with NoDataHandling = IntConstantNoDataCellType) =
    new SparseIntTile(cols, rows, values, cellType)
}
