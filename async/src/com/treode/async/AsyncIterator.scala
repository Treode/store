package com.treode.async

import java.lang.{Iterable => JIterable}
import java.util.{Iterator => JIterator}

trait AsyncIterator [+A] {

  def foreach (cb: Callback [Unit]) (f: (A, Callback [Unit]) => Any)

  def map [B] (f: A => B): AsyncIterator [B] = {
    val self = this
    new AsyncIterator [B] {
      def foreach (cb: Callback [Unit]) (g: (B, Callback [Unit]) => Any): Unit =
        self.foreach (cb) { case (x, cb) => g (f (x), cb) }
    }}

  def filter (p: A => Boolean): AsyncIterator [A] = {
    val self = this
    new AsyncIterator [A] {
      def foreach (cb: Callback [Unit]) (g: (A, Callback [Unit]) => Any): Unit =
        self.foreach (cb) { case (x, cb) => if (p (x)) g (x, cb) else cb() }
    }}

  /** Iterate the entire asynchronous iterator and build a standard sequence. */
  def toSeq (cb: Callback [Seq [A]]): Unit = {
    val builder = Seq.newBuilder [A]
    val ready = new Callback [Unit] {
      def pass (v: Unit): Unit = cb (builder.result)
      def fail (t: Throwable): Unit = cb.fail (t)
    }
    foreach (ready) { case (x, cb) =>
      builder += x
      cb()
    }}
}

object AsyncIterator {

  /** Transform a Scala iterator into an AsyncIterator. */
  def adapt [A] (iter: Iterator [A]) (implicit scheduler: Scheduler): AsyncIterator [A] =
    new AsyncIterator [A] {

      def foreach (cb: Callback [Unit]) (f: (A, Callback [Unit]) => Any) {

        val loop = new Callback [Unit] {

          def pass (v: Unit): Unit = scheduler.execute {
            if (iter.hasNext)
              try {
                f (iter.next, this)
              } catch {
                case t: Throwable =>
                  scheduler.fail (cb, t)
              }
            else
              scheduler.pass (cb, ())
          }

          def fail (t: Throwable): Unit =
            scheduler.fail (cb, t)
        }

        loop()
      }}

  /** Transform a Java iterator into an AsyncIterator. */
  def adapt [A] (iter: JIterator [A]) (implicit scheduler: Scheduler): AsyncIterator [A] =
    new AsyncIterator [A] {

      def foreach (cb: Callback [Unit]) (f: (A, Callback [Unit]) => Any) {

        val loop = new Callback [Unit] {

          def pass (v: Unit): Unit = scheduler.execute {
            if (iter.hasNext)
              try {
                f (iter.next, this)
              } catch {
                case t: Throwable =>
                  scheduler.fail (cb, t)
              }
            else
              scheduler.pass (cb, ())
          }

          def fail (t: Throwable): Unit =
            scheduler.fail (cb, t)
        }

        loop()
      }}

  /** Given asynchronous iterators of sorted items, merge them into single asynchronous iterator
    * that maintains the sort.  Keep duplicate elements, and when two or more input iterators
    * duplicate an element, first list the element from the earlier iterator (that is, by position
    * in `iters`).
    */
  def merge [A] (iters: Seq [AsyncIterator [A]]) (implicit ordering: Ordering [A]): AsyncIterator [A] =
    new MergeIterator (iters)
}
