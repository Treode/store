package com.treode.async

import java.lang.{Iterable => JIterable}
import java.util.{Iterator => JIterator}

trait AsyncIterator [+A] {

  def hasNext: Boolean
  def next (cb: Callback [A])
}

object AsyncIterator {

  /** Transform a Scala iterator into an AsyncIterator. */
  def adapt [A] (iter: Iterator [A]) (implicit scheduler: Scheduler): AsyncIterator [A] =
    new AsyncIterator [A] {

      def hasNext: Boolean = iter.hasNext

      def next (cb: Callback [A]): Unit =
        try {
          scheduler.execute (cb, iter.next)
        } catch {
          case e: Throwable =>
            cb.fail (e)
        }}

  /** Transform a Scala iterable into an AsyncIterator. */
  def adapt [A] (iter: Iterable [A]) (implicit scheduler: Scheduler): AsyncIterator [A] =
    adapt (iter.iterator)

  /** Transform a Java iterator into an AsyncIterator. */
  def adapt [A] (iter: JIterator [A]) (implicit scheduler: Scheduler): AsyncIterator [A] =
    new AsyncIterator [A] {

      def hasNext: Boolean = iter.hasNext

      def next (cb: Callback [A]): Unit =
        try {
          scheduler.execute (cb, iter.next)
        } catch {
          case e: Throwable =>
            scheduler.execute (cb.fail (e))
        }}

  /** Transform a Java iterable into an AsyncIterator. */
  def adapt [A] (iter: JIterable [A]) (implicit scheduler: Scheduler): AsyncIterator [A] =
    adapt (iter.iterator)

  def filter [A] (iter: AsyncIterator [A], cb: Callback [AsyncIterator [A]]) (pred: A => Boolean): Unit =
    FilteredIterator (iter, pred, cb)

  private class Mapper [A, B] (iter: AsyncIterator [A], f: A => B) extends AsyncIterator [B] {

    def hasNext: Boolean = iter.hasNext

    def next (cb: Callback [B]): Unit =
      iter.next (new Callback [A] {

        def pass (v1: A) {
          val v2 = try {
            f (v1)
          } catch {
            case e: Throwable =>
              cb.fail (e)
              return
          }
          cb (v2)
        }

        def fail (t: Throwable) = cb.fail (t)
      })
    }

  def map [A, B] (iter: AsyncIterator [A]) (f: A => B): AsyncIterator [B] =
    new Mapper (iter, f)

  private class Looper [A] (iter: AsyncIterator [A], func: (A, Callback [Unit]) => Any, cb: Callback [Unit])
  extends Callback [A] {

    val next = new Callback [Unit] {

      def pass (v: Unit) {
        try {
          if (iter.hasNext) {
            iter.next (Looper.this)
            return
          }
        } catch {
          case e: Throwable =>
            cb.fail (e)
            return
        }
        cb()
      }

      def fail (t: Throwable) = cb.fail (t)
    }

    def pass (v: A) = func (v, next)
    def fail (t: Throwable) = cb.fail (t)
  }

  def foreach [A] (iter: AsyncIterator [A], cb: Callback [Unit]) (func: (A, Callback [Unit]) => Any): Unit =
    new Looper (iter, func, cb) .next()

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

    def next() {
      try {
        if (iter.hasNext) {
          iter.next (this)
          return
        }
      } catch {
        case e: Throwable =>
          cb.fail (e)
          return
      }
      cb (builder.result)
    }

    def pass (x: A) {
      try {
        builder += x
      } catch {
        case e: Throwable =>
          cb.fail (e)
          return
      }
      next()
    }

    def fail (t: Throwable) = cb.fail (t)
  }

  /** Iterator the entire asynchronous iterator and build a standard sequence. */
  def scan [A] (iter: AsyncIterator [A], cb: Callback [Seq [A]]): Unit =
    new Scanner (iter, cb) .next()
}
