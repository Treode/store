package com.treode.store.timed

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

class TierSpec extends WordSpec {

  implicit class RichPageDescriptor [G, P] (desc: PageDescriptor [G, P]) {

    def readAndPass (pos: Position) (implicit scheduler: StubScheduler, disks: Disks): P = {
      val cb = new CallbackCaptor [P]
      desc.read (pos, cb)
      scheduler.runTasks()
      cb.passed
    }}

  private def setup() = {
    implicit val scheduler = StubScheduler.random()
    implicit val disks = Disks()
    val file = new StubFile
    val config = DiskDriveConfig (16, 12, 1L<<20)
    val cb = new CallbackCaptor [Unit]
    disks.attach (Seq ((Paths.get ("a"), file, config)), cb)
    scheduler.runTasks()
    cb.passed
    (scheduler, disks)
  }

  /** Get the depths of ValueBlocks reached from the index entries. */
  private def getDepths (entries: Iterable [IndexEntry], depth: Int) (
      implicit scheduler: StubScheduler, disks: Disks): Set [Int] =
    entries.map (e => getDepths (e.pos, depth+1)) .fold (Set.empty) (_ ++ _)

  /** Get the depths of ValueBlocks for the tree root at `pos`. */
  private def getDepths (pos: Position, depth: Int) (
      implicit scheduler: StubScheduler, disks: Disks): Set [Int] = {
    TierPage.page.readAndPass (pos) match {
      case b: IndexPage => getDepths (b.entries, depth+1)
      case b: CellPage => Set (depth)
    }}

  /** Check that tree rooted at `pos` has all ValueBlocks at the same depth, expect those under
    * the final index entry.
    */
  private def expectBalanced (pos: Position) (implicit scheduler: StubScheduler, disks: Disks) {
    TierPage.page.readAndPass (pos) match {
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
      implicit scheduler: StubScheduler, disks: Disks): Position = {
    val builder = new TierBuilder (StoreConfig (pageBytes))
    val iter = AsyncIterator.adapt (AllFruits.iterator)
    val added = new CallbackCaptor [Unit]
    AsyncIterator.foreach (iter, added) (builder.add (_, 7, Some (One), _))
    scheduler.runTasks()
    added.passed
    val built = new CallbackCaptor [Position]
    builder.result (built)
    scheduler.runTasks()
    built.passed
  }

  private def read (pos: Position, key: Bytes, time: Long) (
      implicit scheduler: StubScheduler, disks: Disks): Option [TimedCell] = {
    val cb = new CallbackCaptor [Option [TimedCell]]
    Tier.read (pos, key, time, cb)
    scheduler.runTasks()
    cb.passed
  }

  /** Build a sequence of the cells in the tier by using the TierIterator. */
  private def iterateTier (pos: Position) (
      implicit scheduler: StubScheduler, disks: Disks): Seq [TimedCell] = {
    val iter = new CallbackCaptor [TierIterator]
    TierIterator (pos, iter)
    scheduler.runTasks()
    val seq = new CallbackCaptor [Seq [TimedCell]]
    AsyncIterator.scan (iter.passed, seq)
    scheduler.runTasks()
    seq.passed
  }

  private def toSeq (builder: Builder [TimedCell, _], pos: Position) (
      implicit scheduler: StubScheduler, disks: Disks) {
    TierPage.page.readAndPass (pos) match {
      case page: IndexPage =>
        page.entries foreach (e => toSeq (builder, e.pos))
      case page: CellPage =>
        page.entries foreach (builder += _)
    }}

  /** Build a sequence of the cells in the tier using old-fashioned recursion. */
  private def toSeq (pos: Position) (
      implicit scheduler: StubScheduler, disks: Disks): Seq [TimedCell] = {
    val builder = Seq.newBuilder [TimedCell]
    toSeq (builder, pos)
    builder.result
  }

  "The TierBuilder" should {

    "require that added entries are not duplicated" in {
      implicit val (scheduler, disks) = setup()
      val builder = new TierBuilder (StoreConfig (1 << 16))
      builder.add (Apple, 1, None, Callback.ignore)
      intercept [IllegalArgumentException] {
        builder.add (Apple, 1, None, Callback.ignore)
      }}

    "require that added entries are sorted by key" in {
      implicit val (scheduler, disks) = setup()
      val builder = new TierBuilder (StoreConfig (1 << 16))
      builder.add (Orange, 1, None, Callback.ignore)
      intercept [IllegalArgumentException] {
        builder.add (Apple, 1, None, Callback.ignore)
      }}

    "require that added entries are reverse sorted by time" in {
      implicit val (scheduler, disks) = setup()
      val builder = new TierBuilder (StoreConfig (1 << 16))
      builder.add (Apple, 1, None, Callback.ignore)
      intercept [IllegalArgumentException] {
        builder.add (Apple, 2, None, Callback.ignore)
      }}

    "allow properly sorted entries" in {
      implicit val (scheduler, disks) = setup()
      val builder = new TierBuilder (StoreConfig (1 << 16))
      builder.add (Apple, 2, None, Callback.ignore)
      builder.add (Apple, 1, None, Callback.ignore)
      builder.add (Orange, 1, None, Callback.ignore)
    }

    "build a blanced tree with all keys" when {

      def checkBuild (pageBytes: Int) {
        implicit val (scheduler, disks) = setup()
        val pos = buildTier (pageBytes)
        expectBalanced (pos)
        expectResult (AllFruits.toSeq) (toSeq (pos) .map (_.key))
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
        val pos = buildTier (pageBytes)
        expectResult (AllFruits.toSeq) (iterateTier (pos) map (_.key))
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
        implicit val (scheduler, disks) = setup()
        val pos = buildTier (pageBytes)
        expectResult (Some (One)) (read (pos, Apple, 8) .get.value)
        expectResult (Some (One)) (read (pos, Apple, 7) .get.value)
        expectResult (None) (read (pos, Apple, 6))
        expectResult (Some (One)) (read (pos, Orange, 8) .get.value)
        expectResult (Some (One)) (read (pos, Orange, 7) .get.value)
        expectResult (None) (read (pos, Orange, 6))
        expectResult (Some (One)) (read (pos, Watermelon, 8) .get.value)
        expectResult (Some (One)) (read (pos, Watermelon, 7) .get.value)
        expectResult (None) (read (pos, Watermelon, 6))
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
