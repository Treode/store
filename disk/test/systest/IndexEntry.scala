package systest

import com.google.common.primitives.Longs
import com.treode.disk.Position
import com.treode.pickle.Picklers

class IndexEntry (val key: Int, val disk: Int, val offset: Long, val length: Int)
extends Ordered [IndexEntry] {

  def pos = Position (disk, offset, length)

  def byteSize = IndexEntry.pickler.byteSize (this)

  def compare (that: IndexEntry): Int = key compare that.key

  override def toString = s"IndexEntry($key,$disk,$offset,$length)"
}

object IndexEntry extends Ordering [IndexEntry] {

  def apply (key: Int, disk: Int, offset: Long, length: Int): IndexEntry =
    new IndexEntry (key, disk, offset, length)

  def apply (key: Int, pos: Position): IndexEntry =
    new IndexEntry (key, pos.disk, pos.offset, pos.length)

  def compare (x: IndexEntry, y: IndexEntry): Int =
    x compare y

  val pickler = {
    import Picklers._
    wrap (int, uint, ulong, uint)
    .build (v => IndexEntry (v._1, v._2, v._3, v._4))
    .inspect (v => (v.key, v.disk, v.offset, v.length))
  }}
