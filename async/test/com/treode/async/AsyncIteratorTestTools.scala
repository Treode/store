package com.treode.async

import org.scalatest.Assertions

import Assertions.assertResult

object AsyncIteratorTestTools extends AsyncTestTools {

  class DistinguishedException extends Exception

  def adapt [A] (xs: A*) (implicit scheduler: StubScheduler): AsyncIterator [A] =
    AsyncIterator.adapt (xs.iterator)

  def track [A] (iter: AsyncIterator [A]) (f: A => Any): AsyncIterator [A] =
    new AsyncIterator [A] {
      def _foreach (g: (A, Callback [Unit]) => Any): Async [Unit] = {
        iter._foreach { case (x, cb) =>
          f (x)
          g (x, cb)
        }}}

  def failWhen [A] (iter: AsyncIterator [A]) (p: A => Boolean): AsyncIterator [A] =
    new AsyncIterator [A] {
      def _foreach (g: (A, Callback [Unit]) => Any): Async [Unit] = {
        iter._foreach { case (x, cb) =>
          if (p (x)) throw new DistinguishedException
          g (x, cb)
        }}}

  def expectSeq [A] (expected: A*) (actual: AsyncIterator [A]) (implicit s: StubScheduler): Unit =
    assertResult (expected) (actual.toSeq)

  def expectFail [E] (iter: AsyncIterator [_]) (implicit s: StubScheduler, m: Manifest [E]): Unit =
    iter.foreach.f (_ => ()) .fail [E]
}
