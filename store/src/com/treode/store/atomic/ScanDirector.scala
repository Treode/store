package com.treode.store.atomic

import scala.collection.mutable.PriorityQueue
import scala.util.{Failure, Success}

import com.treode.async.{Async, Callback, Fiber}
import com.treode.cluster.Peer
import com.treode.store.{Bytes, Cell, CellIterator, Key, TableId, TimeoutException, TxClock}

import Async.async
import ScanDeputy.{Cells, Point}
import ScanDirector._

private class ScanDirector (
    var prev: Key,
    table: TableId,
    kit: AtomicKit,
    body: Cell => Async [Unit],
    cb: Callback [Unit]
) {

  import kit.{cluster, library, random, scheduler}
  import kit.config.scanBatchBackoff

  sealed abstract class State
  case object Awaiting extends State
  case object Processing extends State
  case object Closed extends State

  val fiber = new Fiber
  val pq = new PriorityQueue [Element]
  var acks = Map.empty [Peer, (Int, Point)]
  var backoff = scanBatchBackoff.iterator
  var state: State = Awaiting

  val port = ScanDeputy.scan.open {
    case (Success ((cells, point)), from) =>
      got (cells, point, from)
    case _ =>
      ()
  }

  val take: Callback [Unit] = { v =>
    fiber.execute {
      v match {
        case Failure (t) if state == Closed =>
          scheduler.fail (cb, t)
        case Failure (t) if state != Closed =>
          state = Closed
          port.close()
          scheduler.fail (cb, t)
        case Success (_) if state == Closed =>
          scheduler.fail (cb, new TimeoutException)
        case Success (_) if quorum =>
          give()
        case _ =>
          backoff = scanBatchBackoff.iterator
          state = Awaiting
          rouse (prev)
      }}}

  rouse (prev)

  def quorum: Boolean = {
    val acks = this.acks
        .filter {case (key, (count, end)) => count > 0 || end.isEmpty}
        .keySet.map (_.id)
    library.atlas.cohorts forall (_.quorum (acks))
  }

  def awaiting: Set [Peer] = {
    val acks = this.acks
        .filter {case (key, (count, end)) => count > 0 || end.isEmpty}
        .keySet.map (_.id)
    library.atlas.cohorts
        .map (_.hosts)
        .fold (Set.empty) (_ ++ _)
        .filterNot (acks contains _)
        .map (cluster.peer (_))
  }

  def give() {
    if (pq.isEmpty) {
      state = Closed
      port.close()
      scheduler.pass (cb, ())
    } else {
      val element = pq.dequeue()
      val (count, end) = acks (element.from)
      assert (count > 0)
      acks += element.from -> (count - 1, end)
      if (count < 5 && end.isDefined)
        ScanDeputy.scan (table, end.get.key, end.get.time) (element.from, port)
      state = Processing
      prev = element.cell.timedKey
      body (element.cell) run (take)
    }}

  def got (cells: Cells, point: Point, from: Peer): Unit =
    fiber.execute {
      if (state == Closed) return
      acks get (from) match {
        case Some ((count, Some (end))) =>
          var count = 0
          for (c <- cells; k = c.timedKey; if prev < k && k < end) {
            pq.enqueue (Element (c, from))
            count += 1
          }
          if (count > 0 || point == None)
            acks += from -> (count, point)
        case Some ((count, None)) =>
          ()
        case None =>
          var count = 0
          for (c <- cells; k = c.timedKey; if prev < k) {
            pq.enqueue (Element (c, from))
            count += 1
          }
          if (count > 0 || point == None)
            acks += from -> (count, point)
      }
      if (state == Awaiting && quorum)
        give()
    }

  def rouse (mark: Key): Unit =
    fiber.execute {
      state match {
        case Closed =>
          ()
        case _ if mark != prev =>
          ()
        case _ if backoff.hasNext =>
          ScanDeputy.scan (table, prev.key, prev.time) (awaiting, port)
          scheduler.delay (backoff.next) (rouse (prev))
        case Awaiting =>
          state = Closed
          port.close()
          scheduler.fail (cb, new TimeoutException)
        case Processing =>
          state = Closed
          port.close()
      }}}

private object ScanDirector {

  case class Element (cell: Cell, from: Peer) extends Ordered [Element] {

    // Reverse the sort for the PriorityQueue.
    def compare (that: Element): Int =
      that.cell compare cell
  }

  object Element extends Ordering [Element] {

    def compare (x: Element, y: Element): Int =
      x compare y
  }

  def scan (table: TableId, key: Bytes, time: TxClock, kit: AtomicKit): CellIterator = {
    val iter = new CellIterator {
      def foreach (f: Cell => Async [Unit]): Async [Unit] =
        async (new ScanDirector (Key (key, time), table, kit, f, _))
    }
    iter.dedupe
  }}
