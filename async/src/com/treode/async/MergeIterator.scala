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

package com.treode.async

import scala.collection.mutable.PriorityQueue
import scala.util.{Failure, Try, Success}

import Async.{guard, async}

private class MergeIterator [A] (
    iters: Seq [BatchIterator [A]]
) (implicit
    order: Ordering [A],
    scheduler: Scheduler
) extends BatchIterator [A] {

  private case class Element (x: A, n: Int, xs: Iterator [A], cb: Callback [Unit])
  extends Ordered [Element] {

    // Reverse the sort for the PriorityQueue.
    def compare (that: Element): Int = {
      val r = order.compare (that.x, x)
      if (r != 0) r else that.n compare n
    }}

  private class Batch (f: Iterable [A] => Async [Unit], cb: Callback [Unit]) {

    val fiber = new Fiber
    val pq = new PriorityQueue [Element]
    var ready = true
    var count = iters.size
    var thrown = Option.empty [Throwable]

    // Merge the next batch from the prioirty queue; must run inside fiber.
    private def _merge(): Iterable [A] = {
      val b = Seq.newBuilder [A]
      while (pq.size == count) {
        val e = pq.dequeue()
        b += e.x
        if (e.xs.hasNext)
          pq.enqueue (e.copy (x = e.xs.next))
        else
          scheduler.pass (e.cb, ())
      }
      b.result
    }

    // Give the next batch to the body; must be run inside fiber.
    private def _give() {
      val xs = _merge()
      if (!xs.isEmpty) {
        ready = false
        scheduler.execute (guard (f (xs)) run (took _))
      }}

    // All input iterators finished.
    private def _finish() {
      ready = false
      scheduler.pass (cb, ())
    }

    // Close body with a failure.
    private def _fail() {
      ready = false
      scheduler.fail (cb, thrown.get)
    }

    // Maybe give the next batch to the body; must run inside fiber.
    private def _next() {
      if (!ready || pq.size < count)
        ()
      else if (!thrown.isEmpty)
        _fail()
      else if (count == 0)
        _finish()
      else
        _give()
    }

    // The body took the batch; it's ready for more.
    def took (x: Try [Unit]): Unit =
      fiber.execute {
        ready = true
        x match {
          case Success (_) =>
            _next()
          case Failure (t) =>
            thrown = Some (t)
            _next()
        }}

    // We got values from an input iterator.
    def got (n: Int, xs: Iterator [A], cbi: Callback [Unit]): Unit =
      if (xs.hasNext)
        fiber.execute {
          pq.enqueue (Element (xs.next, n, xs, cbi))
          _next()
        }
      else
        scheduler.pass (cbi, ())

    // An input iterator finished.
    def close (x: Try [Unit]): Unit =
      fiber.execute {
        assert (count > 0, "MergeIterator was already closed.")
        count -= 1
        x match {
          case Success (_) =>
            _next()
          case Failure (t) =>
            thrown = Some (t)
            _next()
        }}

    if (count == 0)
      scheduler.pass (cb, ())
    for ((i, n) <- iters.zipWithIndex)
      i.batch (b => async (cb => got (n, b.iterator, cb))) run (close (_))
  }

  def batch (f: Iterable [A] => Async [Unit]): Async [Unit] =
    async (new Batch (f, _))
}
