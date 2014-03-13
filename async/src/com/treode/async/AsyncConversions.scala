package com.treode.async

import java.lang.{Iterable => JIterable}
import java.util.{Iterator => JIterator}
import scala.collection.JavaConversions._
import scala.util.{Failure, Random, Success, Try}

trait AsyncConversions {

  implicit class RichCallback [A] (cb: Try [A] => Any) {

    def pass (v: A): Unit = cb (Success (v))

    def fail (t: Throwable) = cb (Failure (t))

    def callback [B] (f: B => A): Callback [B] = {
      case Success (b) => cb (Try (f (b)))
      case Failure (t) => cb (Failure (t))
    }

    def continue [B] (f: B => Any): Callback [B] = {
      case Success (b) =>
        Try (f (b)) match {
          case Success (_) => ()
          case Failure (t) => cb (Failure (t))
        }
      case Failure (t) => cb (Failure (t))
    }

    def defer (f: => Any) {
      try {
        f
      } catch {
        case t: Throwable => cb (Failure (t))
      }}

    def invoke (f: => A): Unit =
      cb (Try (f))

    def timeout (fiber: Fiber, backoff: Backoff) (rouse: => Any) (implicit random: Random):
        TimeoutCallback [A] =
      new TimeoutCallback (fiber, backoff, rouse, cb)

    def leave (f: => Any): Callback [A] = { v =>
      f
      cb (v)
    }}

  implicit class RichIterator [A] (iter: Iterator [A]) {

    def async (implicit scheduler: Scheduler): AsyncIterator [A] =
      AsyncIterator.adapt (iter)
  }

  implicit class RichIterable [A] (iter: Iterable [A]) {

    def async (implicit scheduler: Scheduler): AsyncIterator [A] =
      AsyncIterator.adapt (iter.iterator)

    object latch extends IterableLatch (iter)
  }

  implicit class RichJavaIterator [A] (iter: JIterator [A]) {

    def async (implicit scheduler: Scheduler): AsyncIterator [A] =
      AsyncIterator.adapt (iter)
  }

  implicit class RichJavaIterable [A] (iter: JIterable [A]) {

    def async (implicit scheduler: Scheduler): AsyncIterator [A] =
      AsyncIterator.adapt (iter.iterator)

    object latch extends IterableLatch (iter)
  }}

object AsyncConversions extends AsyncConversions
