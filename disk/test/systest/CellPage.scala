package systest

import java.util.{Arrays, ArrayList}

import com.treode.pickle.{Pickler, Picklers, PickleContext, UnpickleContext}

class CellPage (val entries: Array [Cell]) extends TierPage {

  def get (i: Int): Cell =
    entries (i)

  def find (key: Int): Int = {
    val i = Arrays.binarySearch (entries, Cell (key, None), Cell)
    if (i < 0) -i-1 else i
  }

  def size: Int = entries.size

  def isEmpty: Boolean = entries.size == 0

  def last: Cell = entries (entries.size - 1)

  override def toString =
    s"CellPage(${entries.head.key}, ${entries.last.key})"
}

object CellPage {

  val empty = new CellPage (new Array (0))

  def apply (entries: Array [Cell]): CellPage =
    new CellPage (entries)

  def apply (entries: ArrayList [Cell]): CellPage =
    new CellPage (entries.toArray (empty.entries))

  val pickler = {
    import Picklers._
    val cell = Cell.pickler
    wrap (array (cell)) .build (CellPage.apply _) .inspect (_.entries)
  }}
