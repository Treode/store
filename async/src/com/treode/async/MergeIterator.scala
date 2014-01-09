package com.treode.async

import scala.collection.mutable.PriorityQueue

private class MergeIterator [A] private (implicit order: Ordering [A])
extends AsyncIterator [A] {

  private case class Element (next: A, tier: Int, iter: AsyncIterator [A])
  extends Ordered [Element] {

    // Reverse the sort for the PriorityQueue.
    def compare (that: Element): Int = {
      val r = order.compare (that.next, next)
      if (r != 0) r else that.tier compare tier
    }}

  private object Element extends Ordering [Element] {

    def compare (x: Element, y: Element): Int =
      x compare y
  }

  private val pq = new PriorityQueue [Element]

  private def enqueue (iters: Iterator [AsyncIterator [A]], cb: Callback [AsyncIterator [A]]) {

    if (iters.hasNext) {

      var iter = iters.next
      while (!iter.hasNext && iters.hasNext)
        iter = iters.next

      val loop = new Callback [A] {

        def pass (x: A) {
          pq.enqueue (Element (x, pq.length, iter))
          if (iters.hasNext) {
            iter = iters.next
            while (!iter.hasNext && iters.hasNext)
              iter = iters.next
            if (iter.hasNext)
              iter.next (this)
            else
              cb (MergeIterator.this)
          } else {
            cb (MergeIterator.this)
          }}

        def fail (t: Throwable) = cb.fail (t)
      }

      if (iter.hasNext)
        iter.next (loop)
      else
        cb (MergeIterator.this)

    } else {
      cb (MergeIterator.this)
    }}

  def hasNext: Boolean = !pq.isEmpty

  def next (cb: Callback [A]) {

    val Element (next, tier, iter) = pq.dequeue()

    if (iter.hasNext) {

      iter.next (new Callback [A] {

        def pass (x: A) {
          pq.enqueue (Element (x, tier, iter))
          cb (next)
        }

        def fail (t: Throwable) = cb.fail (t)
      })

    } else {
      cb (next)
    }}}

private object MergeIterator {

  def apply [A] (iters: Iterator [AsyncIterator [A]], cb: Callback [AsyncIterator [A]]) (
      implicit ordering: Ordering [A]): Unit =
    new MergeIterator() .enqueue (iters, cb)
}
