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

import com.treode.async.{Async, Scheduler}
import com.treode.async.implicits._
import com.treode.store._

import Async.guard
import AtomicTestTools._
import AtomicTracker._

class AtomicTracker {

  private var attempted =
    Map .empty [(Long, Long), Set [Int]] .withDefaultValue (Set.empty)

  private var accepted = newTrackedCells

  private def writting (table: Long, key: Long, value: Int): Unit =
    synchronized {
      val tk = (table, key)
      attempted += tk -> (attempted (tk) + value)
    }

  private def writting (op: WriteOp.Update): Unit =
    writting (op.table.id, op.key.long, op.value.int)

  private def writting (ops: Seq [WriteOp.Update]): Unit =
    ops foreach (writting (_))

  private def wrote (table: Long, key: Long, value: Int, time: TxClock): Unit =
    synchronized {
      val tk = (table, key)
      attempted += tk -> (attempted (tk) - value)
      accepted += tk -> (accepted (tk) + ((time, value)))
    }

  private def wrote (op: WriteOp.Update, wt: TxClock): Unit =
    wrote (op.table.id, op.key.long, op.value.int, wt)

  private def wrote (ops: Seq [WriteOp.Update], wt: TxClock): Unit =
    ops foreach (wrote (_, wt))

  private def aborted (table: Long, key: Long, value: Int): Unit =
    synchronized {
      val tk = (table, key)
      attempted += tk -> (attempted (tk) - value)
    }

  private def aborted (op: WriteOp.Update): Unit =
    aborted (op.table.id, op.key.long, op.value.int)

  private def aborted (ops: Seq [WriteOp.Update]): Unit =
    ops foreach (aborted (_))

  private def condition (table: Long, key: Long): TxClock =
    synchronized {
      val vs = accepted ((table, key))
      if (vs.isEmpty)
        TxClock.MinValue
      else
        vs.keySet.max
    }

  private def condition (op: WriteOp.Update): TxClock =
    condition (op.table.id, op.key.long)

  private def condition (ops: Seq [WriteOp.Update]): TxClock =
    ops .map (condition (_)) .max

  def write (host: StubAtomicHost, ops: Seq [WriteOp.Update]) (implicit random: Random): Async [Unit] =
    guard {
      writting (ops)
      val ct = condition (ops)
      for {
        wt <- host.write (random.nextTxId, ct, ops: _*)
      } yield {
        wrote (ops, wt)
      }
    } .recover {
      case _: StaleException => aborted (ops)
      case _: TimeoutException => aborted (ops)
    }

  def batch (
      ntables: Int,
      nkeys: Int,
      nwrites: Int,
      nops: Int,
      hs: StubAtomicHost*
  ) (implicit
      random: Random
  ): Async [Unit] = {
    val khs = for (ks <- random.nextKeys (ntables, nkeys, nwrites, nops); h <- hs) yield (ks, h)
    for ((ks, h) <- khs.latch)
      write (h, random.nextUpdates (ks))
  }

  def batches (
      nbatches: Int,
      ntables: Int,
      nkeys: Int,
      nwrites: Int,
      nops: Int,
      hs: StubAtomicHost*
  ) (implicit
      random: Random,
      scheduler: Scheduler
  ): Async [Unit] =
    for (n <- (0 until nbatches) .async)
      batch (ntables, nkeys, nwrites, nops, hs:_*)

  def read (host: StubAtomicHost, table: Long, key: Long): Async [Int] =
    for {
      found <- host.read (TxClock.MaxValue, ReadOp (TableId (table), Bytes (key)))
    } yield {
      found.head.value.map (_.int) .getOrElse (-1)
    }

  def check (host: StubAtomicHost) (implicit scheduler: Scheduler): Async [Unit] =
    for {
      _ <- for ((tk, vs) <- accepted.async) {
            val expected = attempted (tk) + vs.maxBy (_._1) ._2
            for {
              found <- read (host, tk._1, tk._2)
            } yield {
              assert (
                  expected contains found,
                  s"Expected $tk to be one of $expected, found $found")
            }}
      _ <- for ((tk, vs) <- attempted.async; if !(accepted contains tk)) {
            val expected = vs + -1
            for {
              found <- read (host, tk._1, tk._2)
            } yield {
              assert (
                  expected contains found,
                  s"Expected $tk to be one of $expected, found $found")
            }}
    } yield ()

  def check (cells: TrackedCells) {
    for ((tk, expected) <- accepted) {
      val found = cells (tk)
      val tried = attempted (tk)
      val unexplained = for {
        (time, value) <- found
        if expected.get (time) != Some (value)
        if !(tried contains value)
      } yield (time, value)
      assert (
          unexplained.isEmpty,
          s"Unexplained values for $tk: $unexplained")
    }}}

object AtomicTracker {

  type TrackedCells = Map [(Long, Long), SortedMap [TxClock, Int]]

  def newTrackedCells =
    Map.empty [(Long, Long), SortedMap [TxClock, Int]] .withDefaultValue (SortedMap.empty (TxClock))
}
