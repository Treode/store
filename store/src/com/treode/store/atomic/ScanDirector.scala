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

import scala.collection.mutable.PriorityQueue
import scala.util.{Failure, Try, Success}

import com.treode.async.{Async, Callback, Fiber}, Async.{async, guard}
import com.treode.cluster.{HostId, Peer}
import com.treode.store._

import ScanDeputy.scan
import ScanDirector.Element

private class ScanDirector (
    table: TableId,
    start: Bound [Key],
    window: Window,
    slice: Slice,
    batch: Batch,
    kit: AtomicKit
) extends CellIterator {

  import kit.{cluster, library, random, scheduler}
  import kit.config.scanBatchBackoff

  private class Batch (body: Iterable [Cell] => Async [Unit], cb: Callback [Unit]) {

    val fiber = new Fiber

    // The atlas at the time we began the scan.
    val atlas = library.atlas

    // The data we have now. A null value indicates that this iterator is closed. We use null to
    // permit prompt garbage collection.
    var pq = new PriorityQueue [Element]

    // The deputies we have data from now, including deputies that have finished. When this set
    // forms a quorum, we can give another batch to the client.
    var have = Set.empty [HostId]

    // The deputies that have finished. When this set forms a quorum, we can supply the last
    // batch and then close this iterator.
    var done = Set.empty [HostId]

    // The point we last supplied to the client. On a timeout, this we rouse the deputies using
    // this start point. On receiving data, we filter incoming cells less than this point.
    var last = start

    // The client body is ready for more data.
    var ready = true

    val port = scan.open {
      case (Success ((cells, end)), from) =>
        got (cells, end, from)
      case _ =>
        ()
    }

    _rouse (start, scanBatchBackoff.iterator)

    // Wake up and maybe resend; must be run inside fiber.
    private def _rouse (mark: Bound [Key], backoff: Iterator [Int]) {
      if (pq == null || mark != last) {
        // The iterator is closed or the timeout is old, so ignore it.
      } else if (backoff.hasNext) {
        val need = atlas.awaiting (have) .map (cluster.peer _)
        scan (table, mark, window, slice, batch) (need, port)
        fiber.delay (backoff.next) (_rouse (mark, backoff))
      } else {
        pq = null
        port.close()
        scheduler.fail (cb, new TimeoutException)
      }}

    // Merge the next batch from the prioirty queue; must run inside fiber.
    private def _merge(): Seq [Cell] = {
      val b = Seq.newBuilder [Cell]
      var q = atlas.quorum (have)
      while (q && pq.size > 0) {
        val e = pq.dequeue()
        var x = e.x
        var k = x.timedKey
        while (last >* k && e.xs.hasNext) {
          x = e.xs.next
          k = x.timedKey
        }
        if (last <* k) {
          b += e.x
          last = Bound (k, false)
        }
        if (e.xs.hasNext) {
          pq.enqueue (e.copy (x = e.xs.next))
        } else if (!e.end) {
          scan (table, last, window, slice, batch) (e.from, port)
          have -= e.from.id
          q = atlas.quorum (have)
        } else {
          done += e.from.id
        }}
      b.result
    }

    // Give the next batch to the body; must be run inside fiber.
    private def _give (taken: Boolean) {
      val xs = _merge()
      if (!xs.isEmpty) {
        // We have data to give, and we proactively asked a deputy for new data.
        ready = false
        scheduler.execute (guard (body (xs)) run (took _))
      } else if (taken) {
        // There's nothing to give, no deputies responded while the client body executed.
        val last = this.last
        val backoff = scanBatchBackoff.iterator
        fiber.delay (backoff.next) (_rouse (last, backoff))
      }}

    // All input iterators finished.
    private def _finish() {
      ready = false
      pq = null
      port.close()
      scheduler.pass (cb, ())
    }

    // Maybe give the next batch to the body; must run inside fiber.
    private def _next (taken: Boolean) {
      if (!ready) {
        ()
      } else if (atlas.quorum (done)) {
        _finish()
      } else {
        _give (taken)
      }}

    // The body took the batch; it's ready for more.
    def took (x: Try [Unit]): Unit = fiber.execute {
      require (pq != null)
      x match {
        case Success (_) =>
          ready = true
          _next (true)
        case Failure (t) =>
          pq = null
          scheduler.fail (cb, t)
      }}

    // We got values from a peer.
    def got (cells: Seq [Cell], end: Boolean, from: Peer): Unit = fiber.execute {
      if (pq == null) {
        // Closed; there's to do.
      } else if (!cells.isEmpty) {
        val iter = cells.iterator
        pq.enqueue (Element (iter.next, iter, end, from))
        have += from.id
        _next (false)
      } else if (end) {
        have += from.id
        done += from.id
        _next (false)
      } else {
        scan (table, last, window, slice, batch) (from, port)
      }}}

  def batch (f: Iterable [Cell] => Async [Unit]): Async [Unit] =
    async (new Batch (f, _))
}

private object ScanDirector {

  case class Element (x: Cell, xs: Iterator [Cell], end: Boolean, from: Peer)
  extends Ordered [Element] {

    // Reverse the sort for the PriorityQueue.
    def compare (that: Element): Int =
      Cell.compare (that.x, x)
  }

  def scan (
      table: TableId,
      start: Bound [Key],
      window: Window,
      slice: Slice,
      batch: Batch,
      kit: AtomicKit
  ): CellIterator =
    (new ScanDirector (table, start, window, slice, batch, kit)) .window (window)
}
