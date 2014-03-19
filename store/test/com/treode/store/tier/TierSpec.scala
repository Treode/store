package com.treode.store.tier

import java.nio.file.Paths
import scala.collection.mutable.Builder

import com.treode.async.{AsyncConversions, StubScheduler}
import com.treode.async.io.StubFile
import com.treode.pickle.Picklers
import com.treode.store._
import com.treode.disk.{Disks, DisksConfig, DiskGeometry, Position}
import org.scalatest.WordSpec

import AsyncConversions._
import Cardinals.One
import Fruits._
import TestTable.descriptor
import TierTestTools._

class TierSpec extends WordSpec {

  val ID = 0x1E

  private def setup(): (StubScheduler, Disks) = {
    implicit val scheduler = StubScheduler.random()
    implicit val disksConfig = DisksConfig (0, 14, 1<<24, 1<<16, 10, 1)
    implicit val recovery = Disks.recover()
    val file = new StubFile
    val geometry = DiskGeometry (20, 12, 1<<30)
    val item = (Paths.get ("a"), file, geometry)
    val launch = recovery.attach (Seq (item)) .pass
    launch.launch()
    (scheduler, launch.disks)
  }

  /** Get the depths of ValueBlocks reached from the index entries. */
  private def getDepths (entries: Iterable [IndexEntry], depth: Int) (
      implicit scheduler: StubScheduler, disks: Disks): Set [Int] =
    entries.map (e => getDepths (e.pos, depth+1)) .fold (Set.empty) (_ ++ _)

  /** Get the depths of ValueBlocks for the tree root at `pos`. */
  private def getDepths (pos: Position, depth: Int) (
      implicit scheduler: StubScheduler, disks: Disks): Set [Int] = {
    descriptor.pager.read (pos) .pass match {
      case b: IndexPage => getDepths (b.entries, depth+1)
      case b: TierCellPage => Set (depth)
    }}

  /** Check that tree rooted at `pos` has all ValueBlocks at the same depth, expect those under
    * the final index entry.
    */
  private def expectBalanced (tier: Tier) (implicit scheduler: StubScheduler, disks: Disks) {
    descriptor.pager.read (tier.root) .pass match {
      case b: IndexPage =>
        val ds1 = getDepths (b.entries.take (b.size-1), 1)
        assertResult (1, "Expected lead ValueBlocks at the same depth.") (ds1.size)
        val d = ds1.head
        val ds2 = getDepths (b.last.pos, 1)
        assertResult (true, "Expected final ValueBlocks at depth < $d") (ds2 forall (_ < d))
      case b: TierCellPage =>
        ()
    }}

  /** Build a tier from fruit. */
  private def buildTier (pageBytes: Int) (
      implicit scheduler: StubScheduler, disks: Disks): Tier = {
    implicit val config = StoreConfig (4, pageBytes)
    val builder = new TierBuilder (descriptor, ID, 0)
    AllFruits.async.foreach (builder.add (_, Some (One))) .pass
    builder.result.pass
  }

  /** Build a sequence of the cells in the tier by using the TierIterator. */
  private def iterateTier (tier: Tier) (
      implicit scheduler: StubScheduler, disks: Disks): Seq [TierCell] =
    TierIterator (descriptor, tier.root) .toSeq

  private def toSeq (builder: Builder [TierCell, _], pos: Position) (
      implicit scheduler: StubScheduler, disks: Disks) {
    descriptor.pager.read (pos) .pass match {
      case page: IndexPage =>
        page.entries foreach (e => toSeq (builder, e.pos))
      case page: TierCellPage =>
        page.entries foreach (builder += _)
    }}

  /** Build a sequence of the cells in the tier using old-fashioned recursion. */
  private def toSeq (tier: Tier) (
      implicit scheduler: StubScheduler, disks: Disks): Seq [TierCell] = {
    val builder = Seq.newBuilder [TierCell]
    toSeq (builder, tier.root)
    builder.result
  }

  "The TierBuilder" should {

    "require that added entries are not duplicated" in {
      implicit val (scheduler, disks) = setup()
      implicit val config = StoreConfig (4, 1 << 16)
      val builder = new TierBuilder (descriptor, ID, 0)
      builder.add (Apple, None) .pass
      builder.add (Apple, None) .fail [IllegalArgumentException]
    }

    "require that added entries are sorted by key" in {
      implicit val (scheduler, disks) = setup()
      implicit val config = StoreConfig (4, 1 << 16)
      val builder = new TierBuilder (descriptor, ID, 0)
      builder.add (Orange, None) .pass
      builder.add (Apple, None) .fail [IllegalArgumentException]
    }

    "require that added entries are reverse sorted by time" in {
      implicit val (scheduler, disks) = setup()
      implicit val config = StoreConfig (4, 1 << 16)
      val builder = new TierBuilder (descriptor, ID, 0)
      builder.add (Apple, None) .pass
      builder.add (Apple, None) .fail [IllegalArgumentException]
    }

    "allow properly sorted entries" in {
      implicit val (scheduler, disks) = setup()
      implicit val config = StoreConfig (4, 1 << 16)
      val builder = new TierBuilder (descriptor, ID, 0)
      builder.add (Apple, None) .pass
      builder.add (Orange, None) .pass
      builder.add (Watermelon, None) .pass
    }

    "build a blanced tree with all keys" when {

      def checkBuild (pageBytes: Int) {
        implicit val (scheduler, disks) = setup()
        val tier = buildTier (pageBytes)
        expectBalanced (tier)
        assertResult (AllFruits.toSeq) (toSeq (tier) .map (_.key))
      }

      "the pages are limited to one byte" in {
        checkBuild (1)
      }

      "the pages are limited to 256 bytes" in {
        checkBuild (1 << 6)
      }

      "the pages are limited to 64K" in {
        checkBuild (1 << 16)
      }}}

  "The TierIterator" should {

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
        checkIterator (1 << 6)
      }

      "the pages are limited to 64K" in {
        checkIterator (1 << 16)
      }}}

  "The Tier" should {

    "find the key" when {

      def checkFind (pageBytes: Int) {

        val AppleX = Bytes ("applex")
        val OrangeX = Bytes ("orangex")
        val WatermelonX = Bytes ("watermelonx")

        implicit val (scheduler, disks) = setup()
        val tier = buildTier (pageBytes)

        def ceiling (key: Bytes): Option [Bytes] =
          tier.ceiling (descriptor, key) .pass.map (_.key)

        assertResult (Apple) (ceiling (Apple) .get)
        assertResult (Apricot) (ceiling (AppleX) .get)
        assertResult (Orange) (ceiling (Orange) .get)
        assertResult (Papaya) (ceiling (OrangeX) .get)
        assertResult (Watermelon) (ceiling (Watermelon). get)
        assertResult (None) (ceiling (WatermelonX))
      }

      "the pages are limited to one byte" in {
        checkFind (1)
      }

      "the pages are limited to 256 bytes" in {
        checkFind (1 << 6)
      }

      "the pages are limited to 64K" in {
        checkFind (1 << 16)
      }}}}
