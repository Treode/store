package com.treode.async

import org.scalatest.Assertions

import Assertions._

object AsyncIteratorTestTools {

  class DistinguishedException extends Exception

  def failHasNext [A] (iter: AsyncIterator [A]) (cond: => Boolean): AsyncIterator [A] =
    new AsyncIterator [A] {
      def hasNext: Boolean =
        if (cond) iter.hasNext else throw new DistinguishedException
      def next (cb: Callback [A]): Unit =
        iter.next (cb)
    }

  def failNext [A] (iter: AsyncIterator [A]) (cond: => Boolean): AsyncIterator [A] =
    new AsyncIterator [A] {
      def hasNext: Boolean =
        iter.hasNext
      def next (cb: Callback [A]): Unit =
        if (cond) iter.next (cb) else throw new DistinguishedException
    }

  def trackNext [A] (iter: AsyncIterator [A]) (f: A => Any): AsyncIterator [A] =
    new AsyncIterator [A] {
      def hasNext: Boolean =
        iter.hasNext
      def next (cb: Callback [A]): Unit =
        iter.next (new Callback [A] {
          def pass (v: A) {
            f (v)
            cb (v)
          }
          def fail (t: Throwable): Unit = cb.fail (t)
        })
    }

  def adapt [A] (xs: A*): AsyncIterator [A] =
    AsyncIterator.adapt (xs)

  def expectSeq [A] (xs: A*) (actual: AsyncIterator [A]) {
    val cb = new CallbackCaptor [Seq [A]]
    AsyncIterator.scan (actual, cb)
    expectResult (xs) (cb.passed)
  }

  def expectFail [E] (actual: AsyncIterator [_]) (implicit m: Manifest [E]) {
    val cb = new CallbackCaptor [Seq [_]]
    AsyncIterator.scan (actual, cb)
    cb.failed [E]
  }}
