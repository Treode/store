package com.treode.async

import java.lang.{Iterable => JIterable}
import java.util.{Iterator => JIterator}

trait AsyncIterator [+A] {

  def hasNext: Boolean
  def next (cb: Callback [A])
}

object AsyncIterator {

  /** Transform a Scala iterator into an AsyncIterator. */
  def adapt [A] (iter: Iterator [A]): AsyncIterator [A] =
    new AsyncIterator [A] {
      def hasNext: Boolean = iter.hasNext
      def next (cb: Callback [A]): Unit = cb (iter.next)
    }

  /** Transform a Scala iterable into an AsyncIterator. */
  def adapt [A] (iter: Iterable [A]): AsyncIterator [A] =
    adapt (iter.iterator)

  /** Transform a Java iterator into an AsyncIterator. */
  def adapt [A] (iter: JIterator [A]): AsyncIterator [A] =
    new AsyncIterator [A] {
      def hasNext: Boolean = iter.hasNext
      def next (cb: Callback [A]): Unit = cb (iter.next)
    }

  /** Transform a Java iterable into an AsyncIterator. */
  def adapt [A] (iter: JIterable [A]): AsyncIterator [A] =
    adapt (iter.iterator)

  def filter [A] (iter: AsyncIterator [A], cb: Callback [AsyncIterator [A]]) (pred: A => Boolean): Unit =
    FilteredIterator (iter, pred, cb)

  private class Mapper [A, B] (iter: AsyncIterator [A], f: A => B) extends AsyncIterator [B] {
    def hasNext = iter.hasNext
    def next (cb: Callback [B]) = iter.next (new Callback [A] {
      def pass (v: A) = cb (f (v))
      def fail (t: Throwable) = cb.fail (t)
    })
  }

  def map [A, B] (iter: AsyncIterator [A]) (f: A => B): AsyncIterator [B] =
    new Mapper (iter, f)

  private class Looper [A] (iter: AsyncIterator [A], func: (A, Callback [Unit]) => Any, cb: Callback [Unit])
  extends Callback [A] {

    val next = new Callback [Unit] {
      def pass (v: Unit) {
        if (iter.hasNext)
          iter.next (Looper.this)
        else
          cb()
      }
      def fail (t: Throwable) = cb.fail (t)
    }

    def pass (v: A) = func (v, next)
    def fail (t: Throwable) = cb.fail (t)
  }

  def foreach [A] (iter: AsyncIterator [A], cb: Callback [Unit]) (func: (A, Callback [Unit]) => Any): Unit =
    if (iter.hasNext)
      iter.next (new Looper (iter, func, cb))
    else
      cb()

  /** Given asynchronous iterators of sorted items, merge them into single asynchronous iterator
    * that maintains the sort.  Keep duplicate elements, and when two or more input iterators
    * duplicate an element, first list the element from the earlier iterator (that is, by position
    * in `iters`).
    */
  def merge [A] (iters: Iterator [AsyncIterator [A]], cb: Callback [AsyncIterator [A]]) (
      implicit ordering: Ordering [A]): Unit =
    MergeIterator (iters, cb)

  private class Scanner [A] (iter: AsyncIterator [A], cb: Callback [Seq [A]])
  extends Callback [A] {

    val builder = Seq.newBuilder [A]

    def pass (x: A) {
      builder += x
      if (iter.hasNext)
        iter.next (this)
      else
        cb (builder.result)
    }

    def fail (t: Throwable) = cb.fail (t)
  }

  /** Iterator the entire asynchronous iterator and build a standard sequence. */
  def scan [A] (iter: AsyncIterator [A], cb: Callback [Seq [A]]) {
    if (iter.hasNext)
      iter.next (new Scanner (iter, cb))
    else
      cb (Seq.empty)
  }
}
