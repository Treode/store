package systest

import com.treode.pickle.Picklers

class Cell (val key: Int, val value: Option [Int]) extends Ordered [Cell] {

  def byteSize = 11

  def compare (that: Cell): Int = key compare that.key

  override def hashCode: Int = key.hashCode

  override def equals (other: Any) =
    other match {
      case that: Cell => this.key == that.key
      case _ => false
    }

  override def toString = "Cell" + (key, value)
}

object Cell extends Ordering [Cell] {

  def apply (key: Int, value: Option [Int]): Cell =
    new Cell (key, value)

  def compare (x: Cell, y: Cell): Int =
    x compare y

  val pickler = {
    import Picklers._
    wrap (int, option (int)) build ((apply _).tupled) inspect (v => (v.key, v.value))
  }}
