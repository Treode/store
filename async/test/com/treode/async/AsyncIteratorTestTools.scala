package com.treode.async

import org.scalatest.Assertions

import Assertions._

object AsyncIteratorTestTools {

  implicit val scheduler: Scheduler =
    new Scheduler {
      def execute (task: Runnable): Unit = task.run()
      def delay (millis: Long, task: Runnable): Unit = task.run()
      def at (millis: Long, task: Runnable): Unit = task.run()
      def spawn (task: Runnable): Unit = task.run()
    }

  class DistinguishedException extends Exception

  def adapt [A] (xs: A*): AsyncIterator [A] =
    AsyncIterator.adapt (xs.iterator)

  def track [A] (iter: AsyncIterator [A]) (f: A => Any): AsyncIterator [A] =
    new AsyncIterator [A] {

      def foreach (cb: Callback [Unit]) (g: (A, Callback [Unit]) => Any) {
        iter.foreach (cb) { case (x, cb) =>
          f (x)
          g (x, cb)
        }}}

  def failWhen [A] (iter: AsyncIterator [A]) (p: A => Boolean): AsyncIterator [A] =
    new AsyncIterator [A] {

      def foreach (cb: Callback [Unit]) (g: (A, Callback [Unit]) => Any) {
        iter.foreach (cb) { case (x, cb) =>
          if (p (x)) throw new DistinguishedException
          g (x, cb)
        }}}

  def expectSeq [A] (xs: A*) (actual: AsyncIterator [A]) {
    val cb = CallbackCaptor [Seq [A]]
    actual.toSeq (cb)
    expectResult (xs) (cb.passed)
  }

  def expectFail [E] (actual: AsyncIterator [_]) (implicit m: Manifest [E]) {
    val cb = CallbackCaptor [Seq [_]]
    actual.toSeq (cb)
    cb.failed [E]
  }}
