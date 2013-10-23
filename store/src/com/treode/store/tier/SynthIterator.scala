package com.treode.store.tier

import scala.collection.mutable.PriorityQueue

import com.treode.cluster.concurrent.Callback

private class SynthIterator extends CellIterator {
  import SynthIterator.Element

  private val pq = new PriorityQueue [Element]

  def enqueue (iters: Iterator [CellIterator], cb: Callback [Unit]) {

    if (iters.hasNext) {

      var iter = iters.next
      while (!iter.hasNext && iters.hasNext)
        iter = iters.next

      val loop = new Callback [Cell] {
        def pass (cell: Cell) {
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

  def next (cb: Callback [Cell]) {

    val Element (next, iter) = pq.dequeue()

    if (iter.hasNext) {

      iter.next (new Callback [Cell] {
        def pass (cell: Cell) {
          pq.enqueue (Element (cell, iter))
          cb (next)
        }
        def fail (t: Throwable) = cb.fail (t)
      })

    } else {
      cb (next)
    }}}

private object SynthIterator {

  case class Element (next: Cell, iter: CellIterator) extends Ordered [Element] {

    // Reverse the sort for the PriorityQueue.
    def compare (that: Element): Int =
      that.next compare (next)
  }

  object Element extends Ordering [Element] {

    def compare (x: Element, y: Element): Int =
      x compare y
  }}
