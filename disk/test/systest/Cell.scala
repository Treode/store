package systest

import java.util.{Map => JMap}
import com.treode.pickle.Picklers

class Cell (val key: Int, val value: Option [Int]) extends Ordered [Cell] {

  def byteSize = 11

  def compare (that: Cell): Int = key compare that.key

  override def hashCode: Int = key.hashCode

  override def equals (other: Any) =
    other match {
      case that: Cell => key == that.key
      case _ => false
    }

  override def toString = "Cell" + (key, value)
}

object Cell extends Ordering [Cell] {

  def apply (key: Int, value: Option [Int]): Cell =
    new Cell (key, value)

  def apply (entry: JMap.Entry [Int, Option [Int]]): Cell =
    new Cell (entry.getKey, entry.getValue)

  def compare (x: Cell, y: Cell): Int =
    x compare y

  val pickler = {
    import Picklers._
    wrap (int, option (int))
    .build (v => new Cell (v._1, v._2))
    .inspect (v => (v.key, v.value))
  }}
