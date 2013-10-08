package com.treode.store.tier

import scala.collection.JavaConversions._

import com.treode.cluster.concurrent.Callback
import com.treode.pickle.Picklers
import com.treode.store.{Bytes, Cell, TxClock}
import org.scalatest.WordSpec

class TierSpec extends WordSpec {
  import Fruits.{AllFruits, Apple, Orange, Watermelon}

  private val One = Bytes (Picklers.string, "One")

  /** Get the depths of ValueBlocks reached from the index entries. */
  private def getDepths (disk: DiskStub, entries: Iterable [IndexEntry], depth: Int): Set [Int] =
    entries.map (e => getDepths (disk, e.pos, depth+1)) .fold (Set.empty) (_ ++ _)

  /** Get the depths of ValueBlocks for the tree root at `pos`. */
  private def getDepths (disk: DiskStub, pos: Long, depth: Int): Set [Int] = {
    disk.get (pos) match {
      case b: IndexBlock => getDepths (disk, b.entries, depth+1)
      case b: ValueBlock => Set (depth)
    }}

  /** Check that tree rooted at `pos` has all ValueBlocks at the same depth, expect those under
    * the final index entry.
    */
  private def expectBalanced (disk: DiskStub, pos: Long) {
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

  /** Build a tier from fruit. */
  private def buildTier (disk: DiskStub): Long = {
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

  /** Build a sequence of the values in the tier by using the TierIterator. */
  private def iterateTier (disk: DiskStub, pos: Long): Seq [ValueEntry] = {
    val builder = Seq.newBuilder [ValueEntry]
    TierIterator (disk, pos, Callback.unary { iter: TierIterator =>
      val loop = new Callback [ValueEntry] {
        def apply (e: ValueEntry) {
          builder += e
          if (iter.hasNext)
            iter.next (this)
        }
        def fail (t: Throwable) = throw t
      }
      if (iter.hasNext)
        iter.next (loop)
    })
    builder.result
  }

  "The TierBuilder" should {
    "build a blanced tree with all keys" when {

      def checkBuild (maxBlockSize: Int) {
        val disk = new DiskStub (maxBlockSize)
        val pos = buildTier (disk)
        expectBalanced (disk, pos)
        expectResult (AllFruits.toSeq) (disk.toSeq (pos) .map (_.key))
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

  "The TierIterator" should {
    "iterate all keys" when {

      def checkIterator (maxBlockSize: Int) {
        val disk = new DiskStub (maxBlockSize)
        val pos = buildTier (disk)
        expectResult (AllFruits.toSeq) (iterateTier (disk, pos) map (_.key))
      }

      "the blocks are limited to one byte" in {
        checkIterator (1)
      }
      "the blocks are limited to 256 bytes" in {
        checkIterator (1 << 6)
      }
      "the blocks are limited to 64K" in {
        checkIterator (1 << 16)
      }}}

  "The Tier" should {
    "find the key" when {

      def checkFind (maxBlockSize: Int) {
        val disk = new DiskStub (maxBlockSize)
        val pos = buildTier (disk)
        val tier = new Tier (disk, pos)
        tier.read (Apple, 8, Callback.unary { cell: Option [Cell] =>
          expectResult (Some (One)) (cell.get.value)
        })
        tier.read (Apple, 7, Callback.unary { cell: Option [Cell] =>
          expectResult (Some (One)) (cell.get.value)
        })
        tier.read (Apple, 6, Callback.unary { cell: Option [Cell] =>
          expectResult (None) (cell)
        })
        tier.read (Orange, 8, Callback.unary { cell: Option [Cell] =>
          expectResult (Some (One)) (cell.get.value)
        })
        tier.read (Orange, 7, Callback.unary { cell: Option [Cell] =>
          expectResult (Some (One)) (cell.get.value)
        })
        tier.read (Orange, 6, Callback.unary { cell: Option [Cell] =>
          expectResult (None) (cell)
        })
        tier.read (Watermelon, 8, Callback.unary { cell: Option [Cell] =>
          expectResult (Some (One)) (cell.get.value)
        })
        tier.read (Watermelon, 7, Callback.unary { cell: Option [Cell] =>
          expectResult (Some (One)) (cell.get.value)
        })
        tier.read (Watermelon, 6, Callback.unary { cell: Option [Cell] =>
          expectResult (None) (cell)
        })
      }

      "the blocks are limited to one byte" in {
        try {
        checkFind (1)
        } catch {
          case e: Throwable => e.printStackTrace()
          throw e
        }
      }
      "the blocks are limited to 256 bytes" in {
        checkFind (1 << 6)
      }
      "the blocks are limited to 64K" in {
        checkFind (1 << 16)
      }
    }
  }
}
