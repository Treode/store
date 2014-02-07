package com.treode.store.simple

import java.nio.file.Paths
import scala.collection.mutable.Builder

import com.treode.async.{AsyncIterator, Callback, CallbackCaptor, StubScheduler}
import com.treode.async.io.StubFile
import com.treode.pickle.Picklers
import com.treode.store._
import com.treode.disk.{Disks, DiskDriveConfig, PageDescriptor, Position}
import org.scalatest.WordSpec

import Cardinals.One
import Fruits.{AllFruits, Apple, Orange, Watermelon}

class SimpleTierSpec extends WordSpec {

  private val pager = TierPage.pager (0x4F56A427)

  implicit class RichPageDescriptor [G, P] (desc: PageDescriptor [G, P]) {

    def readAndPass (pos: Position) (implicit scheduler: StubScheduler, disks: Disks): P = {
      val cb = new CallbackCaptor [P]
      desc.read (pos, cb)
      scheduler.runTasks()
      cb.passed
    }}

  private def setup() = {
    implicit val scheduler = StubScheduler.random()
    implicit val recovery = Disks.recover()
    val file = new StubFile
    val config = DiskDriveConfig (20, 12, 1<<30)
    val cb = new CallbackCaptor [Disks]
    recovery.attach (Seq ((Paths.get ("a"), file, config)), cb)
    scheduler.runTasks()
    (scheduler, cb.passed)
  }

  /** Get the depths of ValueBlocks reached from the index entries. */
  private def getDepths (entries: Iterable [IndexEntry], depth: Int) (
      implicit scheduler: StubScheduler, disks: Disks): Set [Int] =
    entries.map (e => getDepths (e.pos, depth+1)) .fold (Set.empty) (_ ++ _)

  /** Get the depths of ValueBlocks for the tree root at `pos`. */
  private def getDepths (pos: Position, depth: Int) (
      implicit scheduler: StubScheduler, disks: Disks): Set [Int] = {
    pager.readAndPass (pos) match {
      case b: IndexPage => getDepths (b.entries, depth+1)
      case b: CellPage => Set (depth)
    }}

  /** Check that tree rooted at `pos` has all ValueBlocks at the same depth, expect those under
    * the final index entry.
    */
  private def expectBalanced (tier: Tier) (implicit scheduler: StubScheduler, disks: Disks) {
    pager.readAndPass (tier.pos) match {
      case b: IndexPage =>
        val ds1 = getDepths (b.entries.take (b.size-1), 1)
        expectResult (1, "Expected lead ValueBlocks at the same depth.") (ds1.size)
        val d = ds1.head
        val ds2 = getDepths (b.last.pos, 1)
        expectResult (true, "Expected final ValueBlocks at depth < $d") (ds2 forall (_ < d))
      case b: CellPage =>
        ()
    }}

  /** Build a tier from fruit. */
  private def buildTier (pageBytes: Int) (
      implicit scheduler: StubScheduler, disks: Disks): Tier = {
    implicit val config = StoreConfig (pageBytes)
    val builder = new TierBuilder (pager, 0)
    val iter = AsyncIterator.adapt (AllFruits.iterator)
    val added = new CallbackCaptor [Unit]
    AsyncIterator.foreach (iter, added) (builder.add (_, Some (One), _))
    scheduler.runTasks()
    added.passed
    val built = new CallbackCaptor [Tier]
    builder.result (built)
    scheduler.runTasks()
    built.passed
  }

  private def read (tier: Tier, key: Bytes) (
      implicit scheduler: StubScheduler, disks: Disks): Option [SimpleCell] = {
    val cb = new CallbackCaptor [Option [SimpleCell]]
    TierReader.read (pager, tier.pos, key, cb)
    scheduler.runTasks()
    cb.passed
  }

  /** Build a sequence of the cells in the tier by using the TierIterator. */
  private def iterateTier (tier: Tier) (
      implicit scheduler: StubScheduler, disks: Disks): Seq [SimpleCell] = {
    val iter = new CallbackCaptor [SimpleIterator]
    TierIterator (pager, tier.pos, iter)
    scheduler.runTasks()
    val seq = new CallbackCaptor [Seq [SimpleCell]]
    AsyncIterator.scan (iter.passed, seq)
    scheduler.runTasks()
    seq.passed
  }

  private def toSeq (builder: Builder [SimpleCell, _], pos: Position) (
      implicit scheduler: StubScheduler, disks: Disks) {
    pager.readAndPass (pos) match {
      case page: IndexPage =>
        page.entries foreach (e => toSeq (builder, e.pos))
      case page: CellPage =>
        page.entries foreach (builder += _)
    }}

  /** Build a sequence of the cells in the tier using old-fashioned recursion. */
  private def toSeq (tier: Tier) (
      implicit scheduler: StubScheduler, disks: Disks): Seq [SimpleCell] = {
    val builder = Seq.newBuilder [SimpleCell]
    toSeq (builder, tier.pos)
    builder.result
  }

  "The simple TierBuilder" should {

    "require that added entries are not duplicated" in {
      implicit val (scheduler, disks) = setup()
      implicit val config = StoreConfig (1 << 16)
      val builder = new TierBuilder (pager, 0)
      builder.add (Apple, None, Callback.ignore)
      intercept [IllegalArgumentException] {
        builder.add (Apple, None, Callback.ignore)
      }}

    "require that added entries are sorted by key" in {
      implicit val (scheduler, disks) = setup()
      implicit val config = StoreConfig (1 << 16)
      val builder = new TierBuilder (pager, 0)
      builder.add (Orange, None, Callback.ignore)
      intercept [IllegalArgumentException] {
        builder.add (Apple, None, Callback.ignore)
      }}

    "require that added entries are reverse sorted by time" in {
      implicit val (scheduler, disks) = setup()
      implicit val config = StoreConfig (1 << 16)
      val builder = new TierBuilder (pager, 0)
      builder.add (Apple, None, Callback.ignore)
      intercept [IllegalArgumentException] {
        builder.add (Apple, None, Callback.ignore)
      }}

    "allow properly sorted entries" in {
      implicit val (scheduler, disks) = setup()
      implicit val config = StoreConfig (1 << 16)
      val builder = new TierBuilder (pager, 0)
      builder.add (Apple, None, Callback.ignore)
      builder.add (Orange, None, Callback.ignore)
      builder.add (Watermelon, None, Callback.ignore)
    }

    "build a blanced tree with all keys" when {

      def checkBuild (pageBytes: Int) {
        implicit val (scheduler, disks) = setup()
        val tier = buildTier (pageBytes)
        expectBalanced (tier)
        expectResult (AllFruits.toSeq) (toSeq (tier) .map (_.key))
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

  "The simple TierIterator" should {

    "iterate all keys" when {

      def checkIterator (pageBytes: Int) {
        implicit val (scheduler, disks) = setup()
        val tier = buildTier (pageBytes)
        expectResult (AllFruits.toSeq) (iterateTier (tier) map (_.key))
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

  "The simple Tier" should {

    "find the key" when {

      def checkFind (pageBytes: Int) {
        implicit val (scheduler, disks) = setup()
        val tier = buildTier (pageBytes)
        expectResult (Some (One)) (read (tier, Apple) .get.value)
        expectResult (Some (One)) (read (tier, Orange) .get.value)
        expectResult (Some (One)) (read (tier, Watermelon) .get.value)
        expectResult (None) (read (tier, One))
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
