package com.treode.async

import java.lang.{Iterable => JIterable}
import java.util.{Iterator => JIterator}
import scala.collection.JavaConversions._
import scala.util.{Failure, Random, Success, Try}

package object implicits {

  implicit class RichCallback [A] (cb: Try [A] => Any) {

    def pass (v: A): Unit = cb (Success (v))

    def fail (t: Throwable) = cb (Failure (t))

    def defer (f: => Any) {
      try {
        f
      } catch {
        case t: Throwable => cb (Failure (t))
      }}

    def callback (f: => Option [A]) {
      Try (f) match {
        case Success (Some (v)) => cb (Success (v))
        case Success (None) => ()
        case Failure (t) => cb (Failure (t))
      }}

    def continue [B] (f: B => Option [A]): Callback [B] = {
      case Success (b) => callback (f (b))
      case Failure (t) => cb (Failure (t))
    }

    def timeout (fiber: Fiber, backoff: Backoff) (rouse: => Any) (implicit random: Random):
        TimeoutCallback [A] =
      new TimeoutCallback (fiber, backoff, rouse, cb)

    def on (s: Scheduler): Callback [A] =
      (v => s.execute (cb, v))

    def ensure (f: => Any): Callback [A] = {
      case Success (v) =>
        cb (Try (f) .map (_ => v))
      case v @ Failure (t1) =>
        try {
          f
        } catch {
          case t2: Throwable => t1.addSuppressed (t2)
        }
        cb (v)
    }

    def recover (f: PartialFunction [Throwable, A]): Callback [A] =
      (v => cb (v recover f))

    def rescue (f: PartialFunction [Throwable, Try [A]]): Callback [A] =
      (v => cb (v recoverWith f))
}

  implicit class RichIterator [A] (iter: Iterator [A]) {

    def async (implicit scheduler: Scheduler): AsyncIterator [A] =
      AsyncIterator.adapt (iter)
  }

  implicit class RichIterable [A] (iter: Iterable [A]) {

    def async (implicit scheduler: Scheduler): AsyncIterator [A] =
      AsyncIterator.adapt (iter.iterator)

    object latch extends IterableLatch (iter, iter.size)
  }

  implicit class RichJavaIterator [A] (iter: JIterator [A]) {

    def async (implicit scheduler: Scheduler): AsyncIterator [A] =
      AsyncIterator.adapt (iter)
  }

  implicit class RichJavaIterable [A] (iter: JIterable [A]) {

    def async (implicit scheduler: Scheduler): AsyncIterator [A] =
      AsyncIterator.adapt (iter.iterator)

    object latch extends IterableLatch (iter, iter.size)
  }}
