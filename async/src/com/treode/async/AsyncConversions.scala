package com.treode.async

import java.lang.{Iterable => JIterable}
import java.util.{Iterator => JIterator}
import scala.collection.JavaConversions._

trait AsyncConversions {

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
