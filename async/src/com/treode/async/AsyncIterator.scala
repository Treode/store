package com.treode.async

import java.lang.{Iterable => JIterable}
import java.util.{Iterator => JIterator}

import Async.whilst

trait AsyncIterator [+A] {

  def _foreach (f: (A, Callback [Unit]) => Any): Async [Unit]

  object foreach {

    def apply (f: A => Async [Unit]): Async [Unit] =
      _foreach ((x, cb) => f (x) run (cb))

    def f (f: A => Any): Async [Unit] =
      _foreach { (x, cb) => f (x); cb.pass() }

    def cb (f: (A, Callback [Unit]) => Any): Async [Unit] =
      _foreach (f)
  }

  def map [B] (f: A => B): AsyncIterator [B] = {
    val self = this
    new AsyncIterator [B] {
      def _foreach (g: (B, Callback [Unit]) => Any): Async [Unit] =
        self._foreach { case (x, cb) => g (f (x), cb) }
    }}

  def filter (p: A => Boolean): AsyncIterator [A] = {
    val self = this
    new AsyncIterator [A] {
      def _foreach (g: (A, Callback [Unit]) => Any): Async [Unit] =
        self._foreach { case (x, cb) => if (p (x)) g (x, cb) else cb.pass() }
    }}}

object AsyncIterator {

  /** Transform a Scala iterator into an AsyncIterator. */
  def adapt [A] (iter: Iterator [A]) (implicit scheduler: Scheduler): AsyncIterator [A] =
    new AsyncIterator [A] {
      def _foreach (f: (A, Callback [Unit]) => Any): Async [Unit] =
        whilst.cb (iter.hasNext) (f (iter.next, _))
    }

  /** Transform a Java iterator into an AsyncIterator. */
  def adapt [A] (iter: JIterator [A]) (implicit scheduler: Scheduler): AsyncIterator [A] =
    new AsyncIterator [A] {
      def _foreach (f: (A, Callback [Unit]) => Any): Async [Unit] =
        whilst.cb (iter.hasNext) (f (iter.next, _))
  }

  /** Given asynchronous iterators of sorted items, merge them into single asynchronous iterator
    * that maintains the sort.  Keep duplicate elements, and when two or more input iterators
    * duplicate an element, first list the element from the earlier iterator (that is, by position
    * in `iters`).
    */
  def merge [A] (iters: Seq [AsyncIterator [A]]) (implicit ordering: Ordering [A]): AsyncIterator [A] =
    new MergeIterator (iters)
}
