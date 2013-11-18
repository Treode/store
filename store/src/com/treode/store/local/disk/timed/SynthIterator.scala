package com.treode.store.local.disk.timed

import scala.collection.mutable.PriorityQueue

import com.treode.concurrent.Callback
import com.treode.store.TimedCell
import com.treode.store.local.TimedIterator

private class SynthIterator extends TimedIterator {
  import SynthIterator.Element

  private val pq = new PriorityQueue [Element]

  def enqueue (iters: Iterator [TimedIterator], cb: Callback [Unit]) {

    if (iters.hasNext) {

      var iter = iters.next
      while (!iter.hasNext && iters.hasNext)
        iter = iters.next

      val loop = new Callback [TimedCell] {
        def pass (cell: TimedCell) {
          pq.enqueue (Element (cell, iter))
          if (iters.hasNext) {
            iter = iters.next
            while (!iter.hasNext && iters.hasNext)
              iter = iters.next
            if (iter.hasNext)
              iter.next (this)
          } else {
            cb()
          }}
        def fail (t: Throwable) = cb.fail (t)
      }

      if (iter.hasNext)
        iter.next (loop)

    } else {
      cb()
    }}

  def hasNext: Boolean = !pq.isEmpty

  def next (cb: Callback [TimedCell]) {

    val Element (next, iter) = pq.dequeue()

    if (iter.hasNext) {

      iter.next (new Callback [TimedCell] {
        def pass (cell: TimedCell) {
          pq.enqueue (Element (cell, iter))
          cb (next)
        }
        def fail (t: Throwable) = cb.fail (t)
      })

    } else {
      cb (next)
    }}}

private object SynthIterator {

  case class Element (next: TimedCell, iter: TimedIterator) extends Ordered [Element] {

    // Reverse the sort for the PriorityQueue.
    def compare (that: Element): Int =
      that.next compare (next)
  }

  object Element extends Ordering [Element] {

    def compare (x: Element, y: Element): Int =
      x compare y
  }}
