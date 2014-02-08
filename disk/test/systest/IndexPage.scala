package systest

import java.util.{Arrays, ArrayList}

import com.treode.pickle.{Pickler, Picklers, PickleContext, UnpickleContext}

class IndexPage (val entries: Array [IndexEntry]) extends TierPage {

  def get (i: Int): IndexEntry =
    entries (i)

  def find (key: Int): Int = {
    val i = Arrays.binarySearch (entries, IndexEntry (key, 0, 0, 0), IndexEntry)
    if (i < 0) -i-1 else i
  }

  def size: Int = entries.size

  def isEmpty: Boolean = entries.size == 0

  def last: IndexEntry = entries (entries.size - 1)

  override def toString =
    s"IndexPage(${entries.head.key}, ${entries.last.key})"
}

object IndexPage {

  val empty = new IndexPage (new Array (0))

  def apply (entries: Array [IndexEntry]): IndexPage =
    new IndexPage (entries)

  def apply (entries: ArrayList [IndexEntry]): IndexPage =
    new IndexPage (entries.toArray (empty.entries))

  val pickler = {
    import Picklers._
    val index = IndexEntry.pickler
    wrap (array (index)) .build (IndexPage.apply _) .inspect (_.entries)
  }}
