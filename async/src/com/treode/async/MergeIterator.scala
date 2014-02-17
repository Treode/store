package com.treode.async

import scala.collection.mutable.PriorityQueue
import scala.language.postfixOps

import Async.async

private class MergeIterator [A] (iters: Seq [AsyncIterator [A]]) (implicit order: Ordering [A])
extends AsyncIterator [A] {

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

  private def _foreach (f: (A, Callback [Unit]) => Any, cb: Callback [Unit]) {

    val pq = new PriorityQueue [Element]
    var count = iters.size
    var thrown = List.empty [Throwable]

    val next: Callback [Unit] =
      new Callback [Unit] {

        def pass (v: Unit): Unit = pq.synchronized {
          val elem = pq.dequeue()
          elem.cb.pass()
        }

        def fail (t: Throwable): Unit = pq.synchronized {
          val elem = pq.dequeue()
          elem.cb.fail (t)
        }}

    def _close() {
      require (count > 0, "MergeIterator was already closed.")
      count -= 1
      if (count > 0 && thrown.isEmpty && count == pq.size)
        f (pq.head.x, next)
      else if (count == pq.size && !thrown.isEmpty)
        cb.fail (MultiException.fit (thrown))
      else if (count == 0 && thrown.isEmpty)
        cb.pass()
    }

    val close: Callback [Unit] =
      new Callback [Unit] {

        def pass (v: Unit): Unit = pq.synchronized {
          _close()
        }

        def fail (t: Throwable): Unit = pq.synchronized {
          thrown ::= t
          _close()
        }}

    def loop (n: Int) (x: A, cb: Callback [Unit]): Unit = pq.synchronized {
      pq.enqueue (Element (x, n, cb))
      if (thrown.isEmpty && count == pq.size)
        f (pq.head.x, next)
    }

    if (count == 0)
      cb.pass()
    for ((iter, n) <- iters zipWithIndex)
      iter.foreach.cb (loop (n) _) run (close)
  }

  def _foreach (f: (A, Callback [Unit]) => Any): Async [Unit] =
    async (_foreach (f, _))
}
