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

package com.treode.store.paxos

import scala.util.Random

import com.treode.async.{Async, BatchIterator, Scheduler}
import com.treode.store._
import org.scalatest.FreeSpec

trait PaxosBehaviors extends StoreClusterChecks {
  this: FreeSpec =>

  private def scan (hosts: Seq [StubPaxosHost]) (implicit scheduler: Scheduler): Async [Seq [Cell]] =
    BatchIterator.merge (hosts map (_.audit)) .toSeq

  private [paxos] def achieveConsensus (nbatches: Int, nputs: Int) (implicit random: Random) = {

    val tracker = new PaxosTracker

    cluster.info (s"achieveConsensus ($nbatches, $nputs)")

    .host (StubPaxosHost)

    .run { implicit scheduler => (h1, h2) =>
      tracker.batches (nbatches, nputs, h1, h2)
    }

    .whilst { hosts =>
      def unsettled = hosts exists (_.unsettled)
      def acceptorsOpen = hosts exists (_.acceptorsOpen)
      def proposersOpen = hosts exists (_.proposersOpen)
      unsettled || acceptorsOpen || proposersOpen
    }

    .verify { implicit scheduler => host =>
      tracker.check (host)
    }

    .whilst { hosts =>
      def unsettled = hosts exists (_.unsettled)
      def acceptorsOpen = hosts exists (_.acceptorsOpen)
      def proposersOpen = hosts exists (_.proposersOpen)
      unsettled || acceptorsOpen || proposersOpen
    }

    .audit { implicit scheduler => hosts =>
      for {
        cells <- scan (hosts)
      } yield {
        tracker.check (cells)
      }}}}
