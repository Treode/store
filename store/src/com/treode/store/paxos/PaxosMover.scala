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

import java.util.concurrent.{TimeoutException => JTimeoutException}
import scala.util.{Failure, Success}

import com.treode.async._
import com.treode.async.implicits._
import com.treode.cluster._
import com.treode.disk.{ObjectId, TypeId}
import com.treode.store._
import com.treode.store.tier.TierTable

import Async.{async, guard, latch, supply, when}
import Callback.ignore
import Cohort.Moving
import PaxosMover.{Batch, Point, Range, Targets, Tracker, move}

private class PaxosMover (kit: PaxosKit) {
  import kit.{acceptors, archive, cluster, disk, library, random, scheduler}
  import kit.config.{moveBatchBackoff, moveBatchBytes, moveBatchEntries}
  import kit.library.{atlas, releaser}

  private val fiber = new Fiber
  private val queue = new AsyncQueue (fiber) (next _)
  private val tracker = new Tracker
  private var callbacks = List.empty [Callback [Unit]]

  queue.launch()

  def split (start: Point.Middle, limit: Point, targets: Targets): Async [(Batch, Point)] =
    disk.join {

      var batch = Map.empty [Int, List [Cell]]
      var entries = 0
      var bytes = 0

      val atlas = library.atlas
      val residents = library.residents

      val iter = archive.iterator (start.start, residents)
      val next = limit

      iter.whilst { cell =>
        entries < moveBatchEntries &&
        bytes < moveBatchBytes &&
        Point.Middle (cell.key, cell.time) < limit
      } { cell =>
        val num = place (atlas, cell.key, cell.time)
        if (targets contains num) {
          batch.get (num) match {
            case Some (cs) => batch += num -> (cell::cs)
            case None => batch += num -> List (cell)
          }
          entries += 1
          bytes += cell.byteSize
        }
        supply (())
      } .map {
        case Some (cell) =>
          (batch, Point.Middle (cell.key, cell.time))
        case None =>
          (batch, next)
      }}

  move.listen { case ((version, cells), from) =>
    if (version < atlas.version - 1 || atlas.version + 1 < version)
      throw new IgnoreRequestException
    val (gen, novel) = archive.receive (cells)
    latch (
        when (!novel.isEmpty) (Acceptors.receive.record (gen, novel)),
        releaser.release()
    ) .map (_ => ())
  }

  private class Sender (version: Int, cells: Seq [Cell], hosts: Set [Peer], cb: Callback [Unit]) {

    val acks = ReplyTracker.settled (hosts map (_.id))

    val port = move.open { case (_, from) =>
      got (from)
    }

    val timer = cb.ensure {
      port.close()
    } .timeout (fiber, moveBatchBackoff) {
      move (version, cells) (acks, port)
    }

    timer.rouse()

    def got (from: Peer) {
      acks += from
      if (acks.quorum)
        timer.pass (())
    }}

  def send (version: Int, cells: Seq [Cell], hosts: Set [Peer]): Async [Unit] =
    async (new Sender (version, cells, hosts, _))

  def send (batch: Batch, targets: Targets): Async [Unit] =
    guard {
      for ((num, cells) <- batch.latch)
        send (targets.version, cells, targets (num))
    }

  def continue (next: Point): Unit =
    fiber.execute (tracker.continue (next))

  def rebalance (start: Point.Middle, limit: Point, targets: Targets): Async [Unit] =
    guard {
      for {
        (batch, next) <- split (start, limit, targets)
        _ <- send (batch, targets)
      } yield continue (next)
    } .recover {
      case _: JTimeoutException => continue (start)
    }

  def next(): Unit =
    (tracker.deque: @unchecked) match {
      case Some ((Range (start: Point.Middle, end), targets)) =>
        queue.begin (rebalance (start, end, targets))
      case None =>
        callbacks foreach (scheduler.pass (_, ()))
        callbacks = List.empty
    }

  def rebalance (targets: Targets): Async [Unit] =
    queue.async { cb =>
      callbacks ::= cb
      tracker.start (targets)
    }}

private object PaxosMover {

  type Batch = Map [Int, Seq [Cell]]

  case class Targets (version: Int, targets: Map [Int, Set [Peer]]) {

    def apply (num: Int): Set [Peer] =
      targets (num)

    def contains (num: Int): Boolean =
      targets contains num

    def isEmpty = targets.isEmpty

    def intersect (other: Targets): Targets = {
      val builder = Map.newBuilder [Int, Set [Peer]]
      for ((num, ps) <- targets)
        other.targets.get (num) match {
          case Some (qs) =>
            val rs = ps intersect qs
            if (!rs.isEmpty) builder += num -> rs
          case None => ()
        }
      new Targets (math.max (version, other.version), builder.result)
    }

    def -- (other: Targets): Targets = {
      val builder = Map.newBuilder [Int, Set [Peer]]
      for ((num, ps) <- targets)
        other.targets.get (num) match {
          case Some (qs) =>
            val rs = ps -- qs
            if (!rs.isEmpty) builder += num -> rs
          case None => builder += num -> ps
        }
      new Targets (math.max (version, other.version), builder.result)
    }}

  object Targets {

    val empty = new Targets (0, Map.empty)

    private def targets (cohort: Cohort, host: HostId): Set [HostId] =
      cohort match {
        case Moving (origin, target) if origin contains host => target - host
        case _ => Set.empty
      }

    def apply (atlas: Atlas) (implicit cluster: Cluster): Targets = {
      val builder = Map.newBuilder [Int, Set [Peer]]
      for {
        (c, i) <- atlas.cohorts.zipWithIndex
        ts = targets (c, cluster.localId)
        if !ts.isEmpty
      }  builder += i -> (ts map (cluster.peer _))
      new Targets (atlas.version, builder.result)
    }}

  sealed abstract class Point extends Ordered [Point]

  object Point extends Ordering [Point] {

    case class Middle (key: Bytes, time: TxClock) extends Point {

      def start: Bound [Key] = Bound.Inclusive (Key (key, time))

      def compare (other: Middle): Int = {
        val r = key compare other.key
        if (r != 0) return r
        other.time compare time
      }

      def compare (other: Point): Int =
        other match {
          case other: Middle => compare (other)
          case End => -1
        }}

    case object End extends Point {

      def compare (other: Point): Int =
        other match {
          case End => 0
          case _ => 1
        }}

    val Start = Middle (Bytes.MinValue, TxClock.MaxValue)

    def compare (x: Point, y: Point): Int =
      x compare y
  }

  case class Range (start: Point, end: Point)

  class Tracker {

    private var targets: Targets = Targets.empty
    private var moving: Targets = Targets.empty
    private var complete = List.empty [(Point, Targets)]

    def deque(): Option [(Range, Targets)] = {
      assert (moving.isEmpty, "Already moving.")
      complete match {
        case _ if targets.isEmpty =>
          None
        case Nil =>
          moving = targets
          Some ((Range (Point.Start, Point.End), targets))
        case (point, moved) :: Nil if targets == moved =>
          moving = targets
          complete = Nil
          Some ((Range (point, Point.End), targets))
        case (p1, m1) :: (tail @ ((p2, m2) :: _)) if targets == m1 =>
          moving = targets
          complete = tail
          Some ((Range (p1, p2), targets -- m2))
        case (point, moved) :: tail =>
          moving = targets
          assert (!moving.isEmpty)
          Some ((Range (Point.Start, point), targets -- moved))
      }}

    def continue (next: Point) {
      complete match {
        case (point, _) :: tail if next < point =>
          complete = (next, moving) :: complete
        case (point, _) :: tail if next == point =>
          complete = (next, moving) :: tail
        case _ :: _ =>
          ()
        case Nil if next == Point.End =>
          targets = Targets.empty
          complete = List.empty
        case Nil =>
          complete = (next, moving) :: Nil
      }
      moving = Targets.empty
    }

    def start (targets: Targets) {
      this.targets = targets
      moving = moving intersect targets
      complete =
        for {
          (p, ts0) <- complete
          ts1 = ts0 intersect targets
          if !ts1.isEmpty
        } yield (p, ts1)
    }}

  val move = {
    import StorePicklers._
    RequestDescriptor (0xFFA56C0DFF8CAD56L, tuple (uint, seq (cell)), unit)
  }}
