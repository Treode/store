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

import com.treode.async.{Async, Scheduler}, Async.guard
import com.treode.async.implicits._
import com.treode.cluster.HostId
import com.treode.cluster.stubs.{StubCluster, StubNetwork}
import com.treode.disk.{DiskLaunch, DiskRecovery}
import com.treode.disk.stubs.edit.{StubDisk, StubDiskChecks}
import com.treode.store.{ChildScheduler, StoreConfig, StoreTestConfig, TimeoutException}
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.FreeSpec

import PaxosTestTools._

class PaxosSpec extends FreeSpec with StubDiskChecks {

  private class PaxosTracker (id: HostId) (implicit config: StoreConfig)
  extends Tracker {

    type Medic = StubPaxosMedic
    type Struct = StubPaxosHost

    private var attempted = Map .empty [Long, Set [Int]] .withDefaultValue (Set.empty)
    private var accepted = Map.empty [Long, Int] .withDefaultValue (-1)

    def recover () (implicit random: Random, scheduler: Scheduler, recovery: DiskRecovery): Medic = {
      implicit val child = new ChildScheduler (scheduler)
      implicit val network = StubNetwork (random)
      new StubPaxosMedic (id)
    }

    def launch (medic: Medic) (implicit launch: DiskLaunch): Async [Struct] =
      for {
        host <- medic.close (launch)
      } yield {
        host.setAtlas (settled (host))
        host
      }

    private def proposing (key: Long, value: Int): Unit =
      synchronized {
        if (!(accepted contains key))
          attempted += key -> (attempted (key) + value)
      }

    private def learned (key: Long, value: Int): Unit =
      synchronized {
        attempted -= key
        if (accepted contains key)
          assert (accepted (key) == value, "Paxos conflicts")
        else
          accepted += key -> value
      }

    def propose (host: StubPaxosHost, key: Long, value: Int): Async [Unit] =
      guard {
        require (value >= 0)
        proposing (key, value)
        for {
          got <- host.propose (key, value)
        } yield {
          learned (key, got.int)
        }
      } .recover {
        case t: TimeoutException => ()
      }

    def batch (nputs: Int, hosts: StubPaxosHost*) (implicit random: Random): Async [Unit] = {
      val khs = for (k <- random.nextKeys (nputs); h <- hosts) yield (k, h)
      for ((k, h) <- khs.latch)
        propose (h, k, random.nextInt (1 << 20))
    }

    def batches (
      nbatches: Int,
      nputs: Int,
      hosts: StubPaxosHost*
    ) (implicit
      random: Random,
      scheduler: Scheduler
    ): Async [Unit] =
      for (_ <- (0 until nbatches) .async)
        batch (nputs, hosts: _*)

    private def read (host: StubPaxosHost, key: Long): Async [Int] =
      host.propose (key, -1)

    def verify (crashed: Boolean, host: Struct) (implicit scheduler: Scheduler): Async [Unit] =
      for {
        _ <- for ((key, value) <- accepted.async) {
          for {
            found <- read (host, key)
          } yield {
            assert (
                found == value,
                s"Expected ${key.long} to be $value, found $found.")
          }}
        _ <- for ((key, values) <- attempted.async; if !(accepted contains key)) {
          for {
            found <- read (host, key)
          } yield {
            assert (
                found == -1 || (values contains found),
                s"Expected ${key.long} to be one of $values, found $found")
          }}
      } yield ()

    override def toString = s"new PaxosTracker (${id.id})"
  }

  private class PaxosPhase (nbatches: Int, nputs: Int) extends Effect [PaxosTracker] {

    def start (
      tracker: PaxosTracker,
      host: StubPaxosHost
    ) (implicit
      random: Random,
      scheduler: Scheduler,
      disk: StubDisk
    ): Async [Unit] =
      tracker.batches (nbatches, nputs, host)

    override def toString = s"new PaxosPhase ($nbatches, $nputs)"
  }


  "The paxos implementation should recover from a crash with" - {

    /*"this" taggedAs (Intensive) in {
      implicit val config = StoreTestConfig.storeConfig()
      onePhase (new PaxosTracker (18), 0xEE05699FCE49C81BL) ((new PaxosPhase (1, 1),2147483647))
    }*/

    for { (name, (nbatches, nputs)) <- Seq (
        "some small batches"     -> (3, 3),
        "many small batches"  -> (10, 3),
        "some big batches" -> (3, 100))
    } name taggedAs (Intensive, Periodic) in {
      implicit val config = StoreTestConfig.storeConfig()
      manyScenarios (new PaxosTracker (0x12), new PaxosPhase (nbatches, nputs))
    }}}
