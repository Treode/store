package com.treode.store.tier

import scala.collection.JavaConversions._
import scala.language.postfixOps

import com.treode.cluster.concurrent.Callback
import com.treode.pickle.Picklers
import com.treode.store.{Bytes, TxClock}
import org.scalatest.WordSpec

class TierSpec extends WordSpec {
  import Fruits.{AllFruits}

  val One = Bytes (Picklers.string, "One")

  /** Get the depths of ValueBlocks reached from the index entries. */
  def getDepths (disk: DiskStub, entries: Iterable [IndexEntry], depth: Int): Set [Int] =
    entries.map (e => getDepths (disk, e.pos, depth+1)) .fold (Set.empty) (_ ++ _)

  /** Get the depths of ValueBlocks for the tree root at `pos`. */
  def getDepths (disk: DiskStub, pos: Long, depth: Int): Set [Int] = {
    disk.get (pos) match {
      case b: IndexBlock => getDepths (disk, b.entries, depth+1)
      case b: ValueBlock => Set (depth)
    }}

  /** Check that tree rooted at `pos` has all ValueBlocks at the same depth, expect those under
    * the final index entry.
    */
  def expectBalanced (disk: DiskStub, pos: Long) {
    disk.get (pos) match {
      case b: IndexBlock =>
        val ds1 = getDepths (disk, b.entries.take (b.size-1), 1)
        expectResult (1, "Expected lead ValueBlocks at the same depth.") (ds1.size)
        val d = ds1.head
        val ds2 = getDepths (disk, b.last.pos, 1)
        expectResult (true, "Expected final ValueBlocks at depth < $d") (ds2 forall (_ < d))
      case b: ValueBlock =>
        ()
    }}

  def buildTier (disk: DiskStub): Long = {
    val builder = new TierBuilder (disk)
    val iter = AllFruits.iterator
    val loop = new Callback [Unit] {
      def apply (v: Unit): Unit =
        if (iter.hasNext)
          builder.add (iter.next, 7, Some (One), this)
      def fail (t: Throwable) = throw t
    }
    loop()
    var pos = 0L
    builder.result (Callback.unary (pos = _))
    pos
  }

  "The TierBuilder" should {
    "build a blanced tree with all keys" when {

      def checkBuild (maxBlockSize: Int) {
        val disk = new DiskStub (maxBlockSize)
        val pos = buildTier (disk)
        expectBalanced (disk, pos)
        expectResult (AllFruits.toSeq) (disk.iterator (pos) map (_.key) toSeq)
      }

      "the blocks are limited to one byte" in {
        checkBuild (1)
      }
      "the blocks are limited to 256 bytes" in {
        checkBuild (1 << 6)
      }
      "the blocks are limited to 64K" in {
        checkBuild (1 << 16)
      }}}

}
