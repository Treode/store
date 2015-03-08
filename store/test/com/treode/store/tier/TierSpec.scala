/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

  private def setup (targetPageBytes: Int = 1 << 20): (StubScheduler, Disk, StoreConfig) = {
    val config = StoreTestConfig (
        checkpointProbability = 0.0,
        compactionProbability = 0.0,
        targetPageBytes = targetPageBytes)
    import config._
    implicit val random = new Random (0)
    implicit val scheduler = StubScheduler.random (random)
    implicit val recovery = StubDisk.recover()
    val diskDrive = new StubDiskDrive
    val launch = recovery.reattach (diskDrive) .expectPass()
    launch.launch()
    (scheduler, launch.disk, config.storeConfig)
  }

  private def newBuilder (est: Long) (
      implicit scheduler: StubScheduler, disk: Disk, config: StoreConfig) =
    new TierBuilder (descriptor, 0, 0, Residents.all, BloomFilter (est, config.falsePositiveProbability))

  /** Get the depths of ValueBlocks reached from the index entries. */
  private def getDepths (entries: Iterable [IndexEntry], depth: Int) (
      implicit scheduler: StubScheduler, disk: Disk): Set [Int] =
    entries.map (e => getDepths (e.pos, depth+1)) .fold (Set.empty) (_ ++ _)

  /** Get the depths of ValueBlocks for the tree root at `pos`. */
  private def getDepths (pos: Position, depth: Int) (
      implicit scheduler: StubScheduler, disk: Disk): Set [Int] = {
    descriptor.pager.read (pos) .expectPass() match {
      case b: IndexPage => getDepths (b.entries, depth+1)
      case b: CellPage => Set (depth)
    }}

  /** Check that tree rooted at `pos` has all ValueBlocks at the same depth, expect those under
    * the final index entry.
    */
  private def expectBalanced (tier: Tier) (implicit scheduler: StubScheduler, disk: Disk) {
    descriptor.pager.read (tier.root) .expectPass() match {
      case b: IndexPage =>
        val ds1 = getDepths (b.entries.take (b.size-1), 1)
        assertResult (1, "Expected lead ValueBlocks at the same depth.") (ds1.size)
        val d = ds1.head
        val ds2 = getDepths (b.last.pos, 1)
        assertResult (true, "Expected final ValueBlocks at depth < $d") (ds2 forall (_ < d))
      case b: CellPage =>
        ()
    }}

  private def buildEmptyTier () (
      implicit scheduler: StubScheduler, disk: Disk, config: StoreConfig): Tier = {
    newBuilder (10) .result.expectPass()
  }

  /** Build a tier from fruit. */
  private def buildTier () (
      implicit scheduler: StubScheduler, disk: Disk, config: StoreConfig): Tier = {
    val builder = newBuilder (AllFruits.length)
    AllFruits.async.foreach (v => builder.add (Cell (v, 1, Some (1)))) .expectPass()
    builder.result.expectPass()
  }

  /** Build a sequence of the cells in the tier by using the TierIterator. */
  private def iterateTier (tier: Tier) (
      implicit scheduler: StubScheduler, disk: Disk): Seq [Cell] =
    TierIterator (descriptor, tier.root) .toSeq.expectPass()

  /** Build a sequence of the cells in the tier by using the TierIterator. */
  private def iterateTier (tier: Tier, key: Bytes, time: TxClock, inclusive: Boolean) (
      implicit scheduler: StubScheduler, disk: Disk): Seq [Cell] =
    TierIterator (descriptor, tier.root, Bound (Key (key, time), inclusive)) .toSeq.expectPass()

  private def toSeq (builder: Builder [Cell, _], pos: Position) (
      implicit scheduler: StubScheduler, disk: Disk) {
    descriptor.pager.read (pos) .expectPass() match {
      case page: IndexPage =>
        page.entries foreach (e => toSeq (builder, e.pos))
      case page: CellPage =>
        page.entries foreach (builder += _)
    }}

  /** Build a sequence of the cells in the tier using old-fashioned recursion. */
  private def toSeq (tier: Tier) (
      implicit scheduler: StubScheduler, disk: Disk): Seq [Cell] = {
    val builder = Seq.newBuilder [Cell]
    toSeq (builder, tier.root)
    builder.result
  }

  "The TierBuilder" should {

    "require that added entries are not duplicated" in {
      implicit val (scheduler, disk, config) = setup()
      val builder = newBuilder (2)
      builder.add (Cell (Apple, 0, None)) .expectPass()
      builder.add (Cell (Apple, 0, None)) .fail [IllegalArgumentException]
    }

    "require that added entries are sorted by key" in {
      implicit val (scheduler, disk, config) = setup()
      val builder = newBuilder (2)
      builder.add (Cell (Orange, 0, None)) .expectPass()
      builder.add (Cell (Apple, 0, None)) .fail [IllegalArgumentException]
    }

    "allow properly sorted entries" in {
      implicit val (scheduler, disk, config) = setup()
      val builder = newBuilder (3)
      builder.add (Cell (Apple, 0, None)) .expectPass()
      builder.add (Cell (Orange, 0, None)) .expectPass()
      builder.add (Cell (Watermelon, 0, None)) .expectPass()
    }

    "track the number of entries and keys" in {
      implicit val (scheduler, disk, config) = setup()
      val builder = newBuilder (3)
      builder.add (Cell (Apple, 3, None)) .expectPass()
      builder.add (Cell (Apple, 1, None)) .expectPass()
      builder.add (Cell (Orange, 5, None)) .expectPass()
      builder.add (Cell (Orange, 3, None)) .expectPass()
      builder.add (Cell (Orange, 1, None)) .expectPass()
      builder.add (Cell (Watermelon, 3, None)) .expectPass()
      val tier = builder.result(). expectPass()
      assertResult (3) (tier.keys)
    }

    "track the bounds on the times" in {
      implicit val (scheduler, disk, config) = setup()
      val builder = newBuilder (3)
      builder.add (Cell (Apple, 3, None)) .expectPass()
      builder.add (Cell (Orange, 5, None)) .expectPass()
      builder.add (Cell (Watermelon, 7, None)) .expectPass()
      val tier = builder.result(). expectPass()
      assertResult (3) (tier.earliest.time)
      assertResult (7) (tier.latest.time)
    }

    "track the number of bytes" in {
      implicit val (scheduler, disk, config) = setup()
      val builder = newBuilder (3)
      builder.add (Cell (Apple, 1, None)) .expectPass()
      val tier = builder.result() .expectPass()
      assertResult (128) (tier.diskBytes)
    }

    "build a balanced tree with all keys" when {

      def checkBuild (pageBytes: Int, expectedDiskBytes: Int) {
        implicit val (scheduler, disk, config) = setup (pageBytes)
        val tier = buildTier()
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
          implicit val (scheduler, disk, config) = setup (pageBytes)
          val tier = buildTier()
          assertResult (AllFruits.toSeq) (iterateTier (tier) map (_.key))
        }

        "the tier is empty" in {
          implicit val (scheduler, disk, config) = setup (1)
          val tier = buildEmptyTier()
          assertResult (Seq.empty) (iterateTier (tier) map (_.key))
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

    "starting from the middle inclusive" should {

      "iterate all remaining keys" when {

        def checkIterator (pageBytes: Int) {
          implicit val (scheduler, disk, config) = setup (pageBytes)
          val tier = buildTier()
          for (start <- AllFruits) {
            val expected = AllFruits.dropWhile (_ < start) .toSeq
            val actual = iterateTier (tier, start, TxClock.MaxValue, true) map (_.key)
            assertResult (expected) (actual)
          }}

        "the tier is empty" in {
          implicit val (scheduler, disk, config) = setup (1)
          val tier = buildEmptyTier()
          assertResult (Seq.empty) (iterateTier (tier, Apple, 1, true) map (_.key))
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

    "starting from the middle exclusive" should {

      "iterate all remaining keys" when {

        def checkIterator (pageBytes: Int) {
          implicit val (scheduler, disk, config) = setup (pageBytes)
          val tier = buildTier()
          for (start <- AllFruits) {
            val expected = AllFruits.dropWhile (_ <= start) .toSeq
            val actual = iterateTier (tier, start, 1, false) map (_.key)
            assertResult (expected) (actual)
          }}

        "the tier is empty" in {
          implicit val (scheduler, disk, config) = setup (1)
          val tier = buildEmptyTier()
          assertResult (Seq.empty) (iterateTier (tier, Apple, TxClock.MaxValue, false) map (_.key))
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
  }

  "The Tier" should {

    "find the key" when {

      def checkFind (pageBytes: Int) {

        implicit val (scheduler, disk, config) = setup (pageBytes)
        val tier = buildTier()

        def get (key: Bytes): Bytes =
          tier.get (descriptor, key, TxClock.MaxValue) .expectPass() .get.key

        assertResult (Apple) (get (Apple))
        assertResult (Orange) (get (Orange))
        assertResult (Watermelon) (get (Watermelon))
        assertResult (None) {
          tier.get (descriptor, Watermelon, TxClock.MinValue) .expectPass()
        }
      }

      "the tier is empty" in {
        implicit val (scheduler, disk, config) = setup (1)
        val tier = buildEmptyTier()
        assertResult (None) {
          tier.get (descriptor, Apple, TxClock.MinValue) .expectPass()
        }}

      "the pages are limited to one byte" in {
        checkFind (1)
      }

      "the pages are limited to 256 bytes" in {
        checkFind (1 << 8)
      }

      "the pages are limited to 64K" in {
        checkFind (1 << 16)
      }}}}
