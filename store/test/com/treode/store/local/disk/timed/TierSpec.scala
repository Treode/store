package com.treode.store.local.disk.timed

import scala.collection.mutable.Builder

import com.treode.async.Callback
import com.treode.pickle.Picklers
import com.treode.store.{Bytes, Cardinals, Fruits, TimedCell, TxClock}
import com.treode.store.local.disk.DiskSystemStub
import org.scalatest.WordSpec

import Cardinals.One
import Fruits.{AllFruits, Apple, Orange, Watermelon}

class TierSpec extends WordSpec {

  /** Get the depths of ValueBlocks reached from the index entries. */
  private def getDepths (disk: DiskSystemStub, entries: Iterable [IndexEntry], depth: Int): Set [Int] =
    entries.map (e => getDepths (disk, e.pos, depth+1)) .fold (Set.empty) (_ ++ _)

  /** Get the depths of ValueBlocks for the tree root at `pos`. */
  private def getDepths (disk: DiskSystemStub, pos: Long, depth: Int): Set [Int] = {
    disk.read (pos) match {
      case b: IndexPage => getDepths (disk, b.entries, depth+1)
      case b: CellPage => Set (depth)
    }}

  /** Check that tree rooted at `pos` has all ValueBlocks at the same depth, expect those under
    * the final index entry.
    */
  private def expectBalanced (disk: DiskSystemStub, pos: Long) {
    disk.read (pos) match {
      case b: IndexPage =>
        val ds1 = getDepths (disk, b.entries.take (b.size-1), 1)
        expectResult (1, "Expected lead ValueBlocks at the same depth.") (ds1.size)
        val d = ds1.head
        val ds2 = getDepths (disk, b.last.pos, 1)
        expectResult (true, "Expected final ValueBlocks at depth < $d") (ds2 forall (_ < d))
      case b: CellPage =>
        ()
    }}

  /** Build a tier from fruit. */
  private def buildTier (disk: DiskSystemStub): Long = {
    val builder = new TierBuilder (disk)
    val iter = AllFruits.iterator
    val loop = new Callback [Unit] {
      def pass (v: Unit): Unit =
        if (iter.hasNext)
          builder.add (iter.next, 7, Some (One), this)
      def fail (t: Throwable) = throw t
    }
    loop()
    var pos = 0L
    builder.result (Callback.unary (pos = _))
    pos
  }

  /** Build a sequence of the cells in the tier by using the TierIterator. */
  private def iterateTier (disk: DiskSystemStub, pos: Long): Seq [TimedCell] = {
    val builder = Seq.newBuilder [TimedCell]
    TierIterator (disk, pos, Callback.unary { iter: TierIterator =>
      val loop = new Callback [TimedCell] {
        def pass (e: TimedCell) {
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

  private def toSeq (disk: DiskSystemStub, builder: Builder [TimedCell, _], pos: Long) {
    disk.read (pos) match {
      case page: IndexPage =>
        page.entries foreach (e => toSeq (disk, builder, e.pos))
      case page: CellPage =>
        page.entries foreach (builder += _)
    }}

  /** Build a sequence of the cells in the tier using old-fashioned recursion. */
  private def toSeq (disk: DiskSystemStub, pos: Long): Seq [TimedCell] = {
    val builder = Seq.newBuilder [TimedCell]
    toSeq (disk, builder, pos)
    builder.result
  }

  "The TierBuilder" should {

    "require that added entries are not duplicated" in {
      val disk = new DiskSystemStub (1 << 16)
      val builder = new TierBuilder (disk)
      builder.add (Apple, 1, None, Callback.unit (()))
      intercept [IllegalArgumentException] {
        builder.add (Apple, 1, None, Callback.unit (()))
      }}

    "require that added entries are sorted by key" in {
      val disk = new DiskSystemStub (1 << 16)
      val builder = new TierBuilder (disk)
      builder.add (Orange, 1, None, Callback.unit (()))
      intercept [IllegalArgumentException] {
        builder.add (Apple, 1, None, Callback.unit (()))
      }}

    "require that added entries are reverse sorted by time" in {
      val disk = new DiskSystemStub (1 << 16)
      val builder = new TierBuilder (disk)
      builder.add (Apple, 1, None, Callback.unit (()))
      intercept [IllegalArgumentException] {
        builder.add (Apple, 2, None, Callback.unit (()))
      }}

    "allow properly sorted entries" in {
      val disk = new DiskSystemStub (1 << 16)
      val builder = new TierBuilder (disk)
      builder.add (Apple, 2, None, Callback.unit (()))
      builder.add (Apple, 1, None, Callback.unit (()))
      builder.add (Orange, 1, None, Callback.unit (()))
    }

    "build a blanced tree with all keys" when {

      def checkBuild (maxPageSize: Int) {
        val disk = new DiskSystemStub (maxPageSize)
        val pos = buildTier (disk)
        expectBalanced (disk, pos)
        expectResult (AllFruits.toSeq) (toSeq (disk, pos) .map (_.key))
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

      def checkIterator (maxPageSize: Int) {
        val disk = new DiskSystemStub (maxPageSize)
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

      def checkFind (maxPageSize: Int) {
        val disk = new DiskSystemStub (maxPageSize)
        val pos = buildTier (disk)
        Tier.read (disk, pos, Apple, 8, Callback.unary { cell: Option [TimedCell] =>
          expectResult (Some (One)) (cell.get.value)
        })
        Tier.read (disk, pos, Apple, 7, Callback.unary { cell: Option [TimedCell] =>
          expectResult (Some (One)) (cell.get.value)
        })
        Tier.read (disk, pos, Apple, 6, Callback.unary { cell: Option [TimedCell] =>
          expectResult (None) (cell)
        })
        Tier.read (disk, pos, Orange, 8, Callback.unary { cell: Option [TimedCell] =>
          expectResult (Some (One)) (cell.get.value)
        })
        Tier.read (disk, pos, Orange, 7, Callback.unary { cell: Option [TimedCell] =>
          expectResult (Some (One)) (cell.get.value)
        })
        Tier.read (disk, pos, Orange, 6, Callback.unary { cell: Option [TimedCell] =>
          expectResult (None) (cell)
        })
        Tier.read (disk, pos, Watermelon, 8, Callback.unary { cell: Option [TimedCell] =>
          expectResult (Some (One)) (cell.get.value)
        })
        Tier.read (disk, pos, Watermelon, 7, Callback.unary { cell: Option [TimedCell] =>
          expectResult (Some (One)) (cell.get.value)
        })
        Tier.read (disk, pos, Watermelon, 6, Callback.unary { cell: Option [TimedCell] =>
          expectResult (None) (cell)
        })
      }

      "the blocks are limited to one byte" in {
        checkFind (1)
      }
      "the blocks are limited to 256 bytes" in {
        checkFind (1 << 6)
      }
      "the blocks are limited to 64K" in {
        checkFind (1 << 16)
      }}}}
