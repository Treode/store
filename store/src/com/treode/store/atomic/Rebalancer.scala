package com.treode.store.atomic

import scala.util.{Failure, Success}

import com.treode.async._
import com.treode.cluster.{Cluster, HostId, Peer, RequestDescriptor}
import com.treode.disk.{ObjectId, TypeId}
import com.treode.store._
import com.treode.store.tier.TierTable

import Async.{async, guard, supply}
import AsyncImplicits._
import Callback.ignore
import Cohort.Moving
import Rebalancer.{Batch, Point, Range, Targets, Tracker, move}

private class Rebalancer (kit: AtomicKit) {
  import kit.{atlas, cluster, place, random, scheduler, tables}
  import kit.config.{rebalanceBackoff, rebalanceBytes, rebalanceEntries}

  private val fiber = new Fiber (scheduler)
  private val queue = AsyncQueue (fiber) (next())
  private var tracker = new Tracker
  private var callbacks = List.empty [Callback [Unit]]

  queue.launch()

  def split (start: Point.Middle, limit: Point, targets: Targets): Async [(TableId, Batch, Point)] = {

    var batch = Map.empty [Int, List [Cell]]
    var entries = 0
    var bytes = 0

    val residents = atlas.residents
    val (table, iter, next) =
      tables.ceiling (start.table) match {
        case Some (table) if table.id == start.table =>
          (table.id, table.iterator (start.key, start.time, residents), Point.Middle (table.id))
        case Some (table) if Point.Middle (table.id) < limit =>
          (table.id, table.iterator (residents), Point.Middle (table.id.id+1))
        case _ =>
          (TableId (0), AsyncIterator.empty [Cell], limit)
      }

    iter.whilst { cell =>
      entries < rebalanceEntries &&
      bytes < rebalanceBytes &&
      Point.Middle (start.table, cell.key, cell.time) < limit
    } { cell =>
      val num = place (table, cell.key)
      if (targets contains num) {
        batch.get (num) match {
          case Some (cs) => batch += num -> (cell::cs)
          case None => batch += num -> List (cell)
        }
        entries += 1
        bytes += cell.byteSize
      }
      supply()
    } .map {
      case Some (cell) =>
        (table, batch, Point.Middle (table, cell.key, cell.time))
      case None =>
        (table, batch, next)
    }}

  move.listen { case ((table, cells), from) =>
    tables.receive (table, cells) run {
      case Success (_) => from.respond()
      case Failure (t) => throw t
    }}

  def send (table: TableId, cells: Seq [Cell], hosts: Set [Peer]): Async [Unit] =
    async { cb =>

      var awaiting = hosts

      val port = move.open { case (_, from) =>
        awaiting -= from
        if (awaiting.isEmpty)
          cb.pass()
      }

      val timer = cb.ensure {
        port.close()
      } .timeout (fiber, rebalanceBackoff) {
        move (table, cells) (awaiting, port)
      }}

  def send (table: TableId, batch: Batch, targets: Targets): Async [Unit] =
    guard {
      for ((num, cells) <- batch.latch.unit)
        send (table, cells, targets (num))
    }

  def continue (next: Point): Unit =
    fiber.execute {
      tracker.continue (next)
    }

  def rebalance (start: Point.Middle, limit: Point, targets: Targets): Async [Unit] = {
    for {
      (table, batch, next) <- split (start, limit, targets)
      _ <- send (table, batch, targets)
    } yield {
      continue (next)
    }}

  def next(): Option [Runnable] =
    (tracker.deque: @unchecked) match {
      case Some ((Range (start: Point.Middle, end), targets)) =>
        queue.run (ignore) (rebalance (start, end, targets))
      case None =>
        callbacks foreach (_.pass())
        callbacks = List.empty
        None
    }

  def rebalance (targets: Targets): Async [Unit] =
    queue.async { cb =>
      callbacks ::= cb
      tracker.start (targets)
    }}

private object Rebalancer {

  type Batch = Map [Int, Seq [Cell]]

  case class Targets (targets: Map [Int, Set [Peer]]) {

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
      new Targets (builder.result)
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
      new Targets (builder.result)
    }}

  object Targets {

    val empty = new Targets (Map.empty)

    private def targets (cohort: Cohort): Set [HostId] =
      cohort match {
        case Moving (origin, target) => target -- origin
        case _ => Set.empty
      }

    def apply (cohorts: Cohorts) (implicit cluster: Cluster): Targets = {
      val builder = Map.newBuilder [Int, Set [Peer]]
      for {
        (c, i) <- cohorts.cohorts.zipWithIndex
        ts = targets (c)
        if !ts.isEmpty
      }  builder += i -> (ts map (cluster.peer _))
      new Targets (builder.result)
    }}

  sealed abstract class Point extends Ordered [Point]

  object Point extends Ordering [Point] {

    case class Middle (table: TableId, key: Bytes, time: TxClock) extends Point {

      def compare (other: Middle): Int = {
        var r = table compare other.table
        if (r != 0) return r
        r = key compare other.key
        if (r != 0) return r
        other.time compare time
      }

      def compare (other: Point): Int =
        other match {
          case other: Middle => compare (other)
          case End => -1
        }}

    object Middle {

      def apply (table: TableId): Middle =
        Middle (table.id, Bytes.empty, TxClock.max)
    }

    case object End extends Point {

      def compare (other: Point): Int =
        other match {
          case End => 0
          case _ => 1
        }}

    val Start = Middle (0, Bytes.empty, TxClock.max)

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
      assert (!moving.isEmpty)
      complete match {
        case (point, _) :: tail if next < point =>
          complete = (next, moving) :: complete
        case (point, _) :: tail if next == point =>
          complete = (next, moving) :: tail
        case (point, _) :: _ =>
          assert (false, "Moved too much.")
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
      complete =
        for {
          (p, ts0) <- complete
          ts1 = ts0 intersect targets
          if !ts1.isEmpty
        } yield (p, ts1)
    }}

  val move = {
    import StorePicklers._
    RequestDescriptor (0xFF580230349D330BL, tuple (tableId, seq (cell)), unit)
  }}
