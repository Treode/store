package com.treode.store.local.disk.simple

import scala.collection.mutable.PriorityQueue

import com.treode.async.Callback
import com.treode.store.SimpleCell
import com.treode.store.local.SimpleIterator

private class SynthIterator extends SimpleIterator {
  import SynthIterator.Element

  private val pq = new PriorityQueue [Element]

  def enqueue (iters: Iterator [SimpleIterator], cb: Callback [Unit]) {

    if (iters.hasNext) {

      var iter = iters.next
      while (!iter.hasNext && iters.hasNext)
        iter = iters.next

      val loop = new Callback [SimpleCell] {
        def pass (cell: SimpleCell) {
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

  def next (cb: Callback [SimpleCell]) {

    val Element (next, tier, iter) = pq.dequeue()

    if (iter.hasNext) {

      iter.next (new Callback [SimpleCell] {
        def pass (cell: SimpleCell) {
          pq.enqueue (Element (cell, tier, iter))
          cb (next)
        }
        def fail (t: Throwable) = cb.fail (t)
      })

    } else {
      cb (next)
    }}}

private object SynthIterator {

  case class Element (next: SimpleCell, tier: Int, iter: SimpleIterator)
  extends Ordered [Element] {

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
