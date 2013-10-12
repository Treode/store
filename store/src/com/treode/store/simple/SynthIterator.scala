package com.treode.store.simple

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
        def apply (cell: Cell) {
          pq.enqueue (Element (cell, pq.size, iter))
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

    val Element (next, tier, iter) = pq.dequeue()

    if (iter.hasNext) {

      iter.next (new Callback [Cell] {
        def apply (cell: Cell) {
          pq.enqueue (Element (cell, tier, iter))
          cb (next)
        }
        def fail (t: Throwable) = cb.fail (t)
      })

    } else {
      cb (next)
    }}}

private object SynthIterator {

  case class Element (next: Cell, tier: Int, iter: CellIterator) extends Ordered [Element] {

    // Reverse the sort for the PriorityQueue.
    def compare (that: Element): Int = {
      val r = that.next compare (next)
      if (r != 0)
        return r
      that.tier compare tier
    }}

  object Element extends Ordering [Element] {

    def compare (x: Element, y: Element): Int =
      x compare y
  }}
