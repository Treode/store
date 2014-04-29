package com.treode.async

import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import org.scalatest.Assertions

import Assertions.assertResult
import Async.supply

object AsyncIteratorTestTools {

  class DistinguishedException extends Exception

  def adapt [A] (xs: A*) (implicit scheduler: StubScheduler): AsyncIterator [A] =
    AsyncIterator.adapt (xs.iterator)

  def track [A] (iter: AsyncIterator [A]) (f: A => Any): AsyncIterator [A] =
    new AsyncIterator [A] {
      def foreach (g: A => Async [Unit]): Async [Unit] = {
        iter.foreach { x =>
          f (x)
          g (x)
        }}}

  def failWhen [A] (iter: AsyncIterator [A]) (p: A => Boolean): AsyncIterator [A] =
    new AsyncIterator [A] {
      def foreach (g: A => Async [Unit]): Async [Unit] = {
        iter.foreach { x =>
          if (p (x)) throw new DistinguishedException
          g (x)
        }}}

  def assertSeq [A] (expected: A*) (actual: AsyncIterator [A]) (implicit s: StubScheduler): Unit =
    assertResult (expected) (actual.toSeq)

  def assertFail [E] (iter: AsyncIterator [_]) (implicit s: StubScheduler, m: Manifest [E]): Unit =
    iter.foreach (_ => supply()) .fail [E]
}
