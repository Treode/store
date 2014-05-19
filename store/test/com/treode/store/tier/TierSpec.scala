package com.treode.store.tier

import scala.collection.mutable.Builder
import scala.util.Random

import com.treode.async.implicits._
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.disk.{Disk, Position}
import com.treode.disk.stubs.{StubDisk, StubDiskDrive}
import com.treode.pickle.Picklers
import com.treode.store._
import org.scalatest.WordSpec

import Fruits._
import TestTable.descriptor
import TierTestTools._

class TierSpec extends WordSpec {

  private def setup(): (StubScheduler, Disk) = {
    implicit val random = new Random (0)
    implicit val scheduler = StubScheduler.random (random)
    implicit val recovery = StubDisk.recover (0.0, 0.0)
    val diskDrive = new StubDiskDrive
    val launch = recovery.attach (diskDrive) .pass
    launch.launch()
    (scheduler, launch.disks)
  }

  private def newBuilder (est: Long) (
      implicit scheduler: StubScheduler, disks: Disk, config: StoreConfig) =
    new TierBuilder (descriptor, 0, 0, Residents.all, BloomFilter (est, config.falsePositiveProbability))

  /** Get the depths of ValueBlocks reached from the index entries. */
  private def getDepths (entries: Iterable [IndexEntry], depth: Int) (
      implicit scheduler: StubScheduler, disks: Disk): Set [Int] =
    entries.map (e => getDepths (e.pos, depth+1)) .fold (Set.empty) (_ ++ _)

  /** Get the depths of ValueBlocks for the tree root at `pos`. */
  private def getDepths (pos: Position, depth: Int) (
      implicit scheduler: StubScheduler, disks: Disk): Set [Int] = {
    descriptor.pager.read (pos) .pass match {
      case b: IndexPage => getDepths (b.entries, depth+1)
      case b: CellPage => Set (depth)
    }}

  /** Check that tree rooted at `pos` has all ValueBlocks at the same depth, expect those under
    * the final index entry.
    */
  private def expectBalanced (tier: Tier) (implicit scheduler: StubScheduler, disks: Disk) {
    descriptor.pager.read (tier.root) .pass match {
      case b: IndexPage =>
        val ds1 = getDepths (b.entries.take (b.size-1), 1)
        assertResult (1, "Expected lead ValueBlocks at the same depth.") (ds1.size)
        val d = ds1.head
        val ds2 = getDepths (b.last.pos, 1)
        assertResult (true, "Expected final ValueBlocks at depth < $d") (ds2 forall (_ < d))
      case b: CellPage =>
        ()
    }}

  /** Build a tier from fruit. */
  private def buildTier (pageBytes: Int) (
      implicit scheduler: StubScheduler, disks: Disk): Tier = {
    implicit val config = StoreTestConfig (targetPageBytes = pageBytes)
    val builder = newBuilder (AllFruits.length)
    AllFruits.async.foreach (v => builder.add (Cell (v, 0, Some (1)))) .pass
    builder.result.pass
  }

  /** Build a sequence of the cells in the tier by using the TierIterator. */
  private def iterateTier (tier: Tier) (
      implicit scheduler: StubScheduler, disks: Disk): Seq [Cell] =
    TierIterator (descriptor, tier.root) .toSeq

  /** Build a sequence of the cells in the tier by using the TierIterator. */
  private def iterateTier (tier: Tier, key: Bytes, time: TxClock) (
      implicit scheduler: StubScheduler, disks: Disk): Seq [Cell] =
    TierIterator (descriptor, tier.root, key, time) .toSeq

  private def toSeq (builder: Builder [Cell, _], pos: Position) (
      implicit scheduler: StubScheduler, disks: Disk) {
    descriptor.pager.read (pos) .pass match {
      case page: IndexPage =>
        page.entries foreach (e => toSeq (builder, e.pos))
      case page: CellPage =>
        page.entries foreach (builder += _)
    }}

  /** Build a sequence of the cells in the tier using old-fashioned recursion. */
  private def toSeq (tier: Tier) (
      implicit scheduler: StubScheduler, disks: Disk): Seq [Cell] = {
    val builder = Seq.newBuilder [Cell]
    toSeq (builder, tier.root)
    builder.result
  }

  "The TierBuilder" should {

    "require that added entries are not duplicated" in {
      implicit val (scheduler, disks) = setup()
      implicit val config = StoreTestConfig()
      val builder = newBuilder (2)
      builder.add (Cell (Apple, 0, None)) .pass
      builder.add (Cell (Apple, 0, None)) .fail [IllegalArgumentException]
    }

    "require that added entries are sorted by key" in {
      implicit val (scheduler, disks) = setup()
      implicit val config = StoreTestConfig()
      val builder = newBuilder (2)
      builder.add (Cell (Orange, 0, None)) .pass
      builder.add (Cell (Apple, 0, None)) .fail [IllegalArgumentException]
    }

    "allow properly sorted entries" in {
      implicit val (scheduler, disks) = setup()
      implicit val config = StoreTestConfig()
      val builder = newBuilder (3)
      builder.add (Cell (Apple, 0, None)) .pass
      builder.add (Cell (Orange, 0, None)) .pass
      builder.add (Cell (Watermelon, 0, None)) .pass
    }

    "track the number of entries and keys" in {
      implicit val (scheduler, disks) = setup()
      implicit val config = StoreTestConfig()
      val builder = newBuilder (3)
      builder.add (Cell (Apple, 3, None)) .pass
      builder.add (Cell (Apple, 1, None)) .pass
      builder.add (Cell (Orange, 5, None)) .pass
      builder.add (Cell (Orange, 3, None)) .pass
      builder.add (Cell (Orange, 1, None)) .pass
      builder.add (Cell (Watermelon, 3, None)) .pass
      val tier = builder.result(). pass
      assertResult (3) (tier.keys)
    }

    "track the bounds on the times" in {
      implicit val (scheduler, disks) = setup()
      implicit val config = StoreTestConfig()
      val builder = newBuilder (3)
      builder.add (Cell (Apple, 3, None)) .pass
      builder.add (Cell (Orange, 5, None)) .pass
      builder.add (Cell (Watermelon, 7, None)) .pass
      val tier = builder.result(). pass
      assertResult (3) (tier.earliest.time)
      assertResult (7) (tier.latest.time)
    }

    "track the number of bytes" in {
      implicit val (scheduler, disks) = setup()
      implicit val config = StoreTestConfig()
      val builder = newBuilder (3)
      builder.add (Cell (Apple, 1, None)) .pass
      val tier = builder.result(). pass
      assertResult (128) (tier.diskBytes)
    }

    "build a balanced tree with all keys" when {

      def checkBuild (pageBytes: Int, expectedDiskBytes: Int) {
        implicit val (scheduler, disks) = setup()
        val tier = buildTier (pageBytes)
        expectBalanced (tier)
        assertResult (AllFruits.toSeq) (toSeq (tier) .map (_.key))
        assertResult (AllFruits.length) (tier.keys)
        assertResult (AllFruits.length) (tier.entries)
        assertResult (expectedDiskBytes) (tier.diskBytes)
      }

      "the pages are limited to one byte" in {
        checkBuild (1, 9024)
      }

      "the pages are limited to 256 bytes" in {
        checkBuild (1 << 8, 1856)
      }

      "the pages are limited to 64K" in {
        checkBuild (1 << 16, 1216)
      }}}

  "The TierIterator" when {

    "starting from the beginning" should {

      "iterate all keys" when {

        def checkIterator (pageBytes: Int) {
          implicit val (scheduler, disks) = setup()
          val tier = buildTier (pageBytes)
          assertResult (AllFruits.toSeq) (iterateTier (tier) map (_.key))
        }

        "the pages are limited to one byte" in {
          checkIterator (1)
        }

        "the pages are limited to 256 bytes" in {
          checkIterator (1 << 8)
        }

        "the pages are limited to 64K" in {
          checkIterator (1 << 16)
        }}}

    "starting from the middle" should {

      "iterate all remaining keys" when {

        def checkIterator (pageBytes: Int) {
          implicit val (scheduler, disks) = setup()
          val tier = buildTier (pageBytes)
          for (start <- AllFruits) {
            val expected = AllFruits.dropWhile (_ != start) .toSeq
            val actual = iterateTier (tier, start, 0) map (_.key)
            assertResult (expected) (actual)
          }}

        "the pages are limited to one byte" in {
          checkIterator (1)
        }

        "the pages are limited to 256 bytes" in {
          checkIterator (1 << 8)
        }

        "the pages are limited to 64K" in {
          checkIterator (1 << 16)
        }}}
  }

  "The Tier" should {

    "find the key" when {

      def checkFind (pageBytes: Int) {

        implicit val (scheduler, disks) = setup()
        val tier = buildTier (pageBytes)

        def get (key: Bytes): Bytes =
          tier.get (descriptor, key, TxClock.max) .pass.get.key

        assertResult (Apple) (get (Apple))
        assertResult (Orange) (get (Orange))
        assertResult (Watermelon) (get (Watermelon))
      }

      "the pages are limited to one byte" in {
        checkFind (1)
      }

      "the pages are limited to 256 bytes" in {
        checkFind (1 << 8)
      }

      "the pages are limited to 64K" in {
        checkFind (1 << 16)
      }}}}
