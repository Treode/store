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

import com.treode.async.{Async, Scheduler}
import com.treode.async.implicits._
import com.treode.async.stubs.StubScheduler
import com.treode.store.{Bytes, Cell, TimeoutException}

import Async.guard
import PaxosTestTools._

class PaxosTracker {

  private var attempted = Map .empty [Long, Set [Int]] .withDefaultValue (Set.empty)
  private var accepted = Map.empty [Long, Int] .withDefaultValue (-1)

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

  def batch (nputs: Int, hs: StubPaxosHost*) (implicit random: Random): Async [Unit] = {
    val khs = for (k <- random.nextKeys (nputs); h <- hs) yield (k, h)
    for ((k, h) <- khs.latch)
      propose (h, k, random.nextInt (1<<20))
  }

  def batches (nbatches: Int, nputs: Int, hs: StubPaxosHost*) (
      implicit random: Random, scheduler: Scheduler): Async [Unit] =
    for (n <- (0 until nbatches) .async)
      batch (nputs, hs:_*)

  def read (host: StubPaxosHost, key: Long): Async [Int] =
    host.propose (key, -1)

  def check (host: StubPaxosHost) (implicit scheduler: Scheduler): Async [Unit] =
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

  def check (cells: Seq [Cell]) {
    val all = cells.map (c => (c.key.long, c.value.get.int)) .toMap.withDefaultValue (-1)
    for ((key, value) <- accepted) {
      val found = all (key)
      assert (
          found == value,
          s"Expected ${key.long} to be $value, found $found.")
    }
    for ((key, found) <- all; if !(accepted contains key)) {
      val values = attempted (key) + -1
      assert (
          values contains found,
          s"Expected $key to be one of $values, found $found")

    }}}
