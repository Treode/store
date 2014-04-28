package com.treode.async

import scala.collection.mutable.PriorityQueue
import scala.language.postfixOps
import scala.util.{Failure, Success}

import com.treode.async.implicits._

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

  def foreach (f: A => Async [Unit]): Async [Unit] = async { cb =>

    val pq = new PriorityQueue [Element]
    var count = iters.size
    var thrown = List.empty [Throwable]

    val next: Callback [Unit] = { v =>
      pq.synchronized {
        val elem = pq.dequeue()
        elem.cb (v)
      }}

    def _close() {
      require (count > 0, "MergeIterator was already closed.")
      count -= 1
      if (count > 0 && thrown.isEmpty && count == pq.size)
        cb.defer (f (pq.head.x) run (next))
      else if (count == pq.size && !thrown.isEmpty)
        cb.fail (MultiException.fit (thrown))
      else if (count == 0 && thrown.isEmpty)
        cb.pass()
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

    def loop (n: Int) (x: A): Async [Unit] = async { cbi => pq.synchronized {
      pq.enqueue (Element (x, n, cbi))
      if (thrown.isEmpty && count == pq.size)
        cb.defer (f (pq.head.x) run (next))
    }}

    if (count == 0)
      cb.pass()
    for ((iter, n) <- iters zipWithIndex)
      iter.foreach (loop (n)) run (close)
  }}
