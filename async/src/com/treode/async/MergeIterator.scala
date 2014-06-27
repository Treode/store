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
import scala.language.postfixOps
import scala.util.{Failure, Success}

import Async.{guard, async}

private class MergeIterator [A] (
    iters: Seq [AsyncIterator [A]]
) (implicit 
    order: Ordering [A],
    scheduler: Scheduler
) extends AsyncIterator [A] {

  private case class Element (x: A, tier: Int, cb: Callback [Unit])
  extends Ordered [Element] {

    // Reverse the sort for the PriorityQueue.
    def compare (that: Element): Int = {
      val r = order.compare (that.x, x)
      if (r != 0) r else that.tier compare tier
    }}

  private object Element extends Ordering [Element] {

    def compare (x: Element, y: Element): Int =
      x compare y
  }

  def foreach (f: A => Async [Unit]): Async [Unit] = async { cb =>

    val pq = new PriorityQueue [Element]
    var count = iters.size
    var thrown = List.empty [Throwable]

    def _next(): Unit =
      scheduler.execute {
        guard {
          f (pq.head.x)
        } run { v =>
          pq.synchronized {
            pq.dequeue().cb (v)
          }}}

    def _close() {
      assert (count > 0, "MergeIterator was already closed.")
      count -= 1
      if (count == pq.size && count > 0 && thrown.isEmpty)
        _next()
      else if (count == pq.size && !thrown.isEmpty)
        scheduler.fail (cb, MultiException.fit (thrown))
      else if (count == 0 && thrown.isEmpty)
        scheduler.pass (cb, ())
    }

    val close: Callback [Unit] = {
      case Success (v) =>
        pq.synchronized {
          _close()
        }
      case Failure (t) =>
        pq.synchronized {
          thrown ::= t
          _close()
        }}

    def loop (n: Int) (x: A): Async [Unit] = async { cbi => 
      pq.synchronized {
        pq.enqueue (Element (x, n, cbi))
        if (count == pq.size)
          if (thrown.isEmpty)
            _next()
          else
            scheduler.fail (cb, MultiException.fit (thrown))
      }}

    if (count == 0)
      scheduler.pass (cb, ())
    for ((iter, n) <- iters zipWithIndex)
      iter.foreach (loop (n)) run (close)
  }}
