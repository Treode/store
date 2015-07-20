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

package com.treode.store.atomic

import scala.collection.SortedMap
import scala.util.Random

import com.treode.async.{Async, BatchIterator, Scheduler}
import com.treode.async.implicits._
import com.treode.store._
import org.scalatest.FreeSpec

import Async.supply
import AtomicTracker._

trait AtomicBehaviors extends StoreClusterChecks {
  this: FreeSpec =>

  private def scan (ntables: Int, nslices: Int, host: StubAtomicHost) (implicit scheduler: Scheduler) = {
    var cells = newTrackedCells
    for {
      _ <-
        for {
          id <- (0L until ntables) .async
          slice <- (0 until nslices) .async
          params = ScanParams (table = id, slice = Slice (slice, nslices))
          c <- host.scan (params)
        } supply {
          val tk = (id, c.key.long)
          cells += tk -> (cells (tk) + ((c.time, c.value.get.int)))
        }
    } yield {
      cells
    }}

  private def audit (hosts: Seq [StubAtomicHost]) (implicit scheduler: Scheduler) = {
    val ordering = Ordering.Tuple2 [TableId, Cell]
    val iter = BatchIterator.merge (hosts map (_.audit)) (ordering, scheduler)
    var cells = newTrackedCells
    for {
      _ <-
        for ((t, c) <- iter)
          supply {
            val tk = (t.id, c.key.long)
            cells += tk -> (cells (tk) + ((c.time, c.value.get.int)))
          }
    } yield {
      cells
    }}

  private [atomic] def issueAtomicWrites (
      nbatches: Int,
      ntables: Int,
      nkeys: Int,
      nwrites: Int,
      nops: Int
  ) (implicit
      random: Random
  ) = {

    val tracker = new AtomicTracker

    cluster.info (s"issueAtomicWrites ($nbatches, $ntables, $nkeys, $nwrites, $nops)")

    .host (StubAtomicHost)

    .run { implicit scheduler => (h1, h2) =>
      tracker.batches (nbatches, ntables, nkeys, nwrites, nops, h1)
    }

    .whilst { hosts =>
      def unsettled = hosts exists (_.unsettled)
      def deputiesOpen = hosts exists (_.deputiesOpen)
      unsettled || deputiesOpen
    }

    .verify { implicit scheduler => host =>
      tracker.check (host)
    }

    .whilst { hosts =>
      def unsettled = hosts exists (_.unsettled)
      def deputiesOpen = hosts exists (_.deputiesOpen)
      unsettled || deputiesOpen
    }

    .audit { implicit scheduler => hosts =>
      for {
        cells <- audit (hosts)
      } yield {
        tracker.check (cells)
      }}}

  private [atomic] def scanWholeDatabase (nslices: Int) (implicit random: Random) = {

    val ntables = 100
    val tracker = new AtomicTracker

    cluster.info (s"scanWholeDatabase()")

    .host (StubAtomicHost)

    .setup { implicit scheduler => h1 =>
      tracker.batches (3, ntables, 10000, 3, 10, h1)
    }

    .run { implicit scheduler => (h1, h2) =>
      for {
        cells <- scan (ntables, nslices, h1)
      } yield {
        tracker.check (cells)
      }}

    .audit { implicit scheduler => hosts =>
      supply (())
    }}}
