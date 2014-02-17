package com.treode

import java.lang.{Iterable => JIterable}
import java.util.{Iterator => JIterator}
import scala.language.experimental.macros
import scala.reflect.macros.Context

package object async {

  implicit class RichIterator [A] (iter: Iterator [A]) {

    def async (implicit scheduler: Scheduler): AsyncIterator [A] =
      AsyncIterator.adapt (iter)
  }

  implicit class RichIterable [A] (iter: Iterable [A]) {

    def async (implicit scheduler: Scheduler): AsyncIterator [A] =
      AsyncIterator.adapt (iter.iterator)
  }

  implicit class RichJavaIterator [A] (iter: JIterator [A]) {

    def async (implicit scheduler: Scheduler): AsyncIterator [A] =
      AsyncIterator.adapt (iter)
  }

  implicit class RichJavaIterable [A] (iter: JIterable [A]) {

    def async (implicit scheduler: Scheduler): AsyncIterator [A] =
      AsyncIterator.adapt (iter.iterator)
  }

  def _continue1 [A: c.WeakTypeTag]
      (c: Context) (cb: c.Expr [Callback [_]]) (f: c.Expr [A => Any]): c.Expr [Callback [A]] = {
    import c.universe._
    reify {
      new Callback [A] {
        private val _cb = cb.splice
        def pass (v: A) {
          try {
            f.splice.apply (v)
          } catch {
            case t: Throwable =>
              _cb.fail (t)
          }}
        def fail (t: Throwable): Unit =
          _cb.fail (t)
      }}}

  def continue [A] (cb: Callback [_]) (f: A => Any): Callback [A] =
    macro _continue1 [A]

  def _callback1 [A: c.WeakTypeTag]
      (c: Context) (f: c.Expr [A => Any]): c.Expr [Callback [A]] = {
    import c.universe._
    reify {
      new Callback [A] {
        def pass (v: A): Unit = f.splice (v)
        def fail (t: Throwable): Unit = throw t
      }}}

  def callback [A] (f: A => Any): Callback [A] =
    macro _callback1 [A]

  def _callback2 [A: c.WeakTypeTag, B: c.WeakTypeTag]
      (c: Context) (cb: c.Expr [Callback [A]]) (f: c.Expr [B => A]): c.Expr [Callback [B]] = {
    import c.universe._
    reify {
      new Callback [B] {
        private val _cb = cb.splice
        def pass (b: B) {
          val a: A = try {
            f.splice.apply (b)
          } catch {
            case t: Throwable =>
              _cb.fail (t)
              return
          }
          _cb.pass (a)
        }
        def fail (t: Throwable): Unit =
          _cb.fail (t)
      }}}

  def callback [A, B] (cb: Callback [A]) (f: B => A): Callback [B] =
    macro _callback2 [A, B]

  def _defer1 [A: c.WeakTypeTag]
      (c: Context) (cb: c.Expr [Callback [A]]) (f: c.Expr [Any]): c.Expr [Unit] = {
    import c.universe._
    reify {
      try {
        f.splice
      } catch {
        case t: Throwable =>
          cb.splice.fail (t)
      }}}

  def defer [A] (cb: Callback [A]) (f: Any): Unit =
    macro _defer1 [A]

  def _invoke1 [A: c.WeakTypeTag]
      (c: Context) (cb: c.Expr [Callback [A]]) (f: c.Expr [A]): c.Expr [Unit] = {
    import c.universe._
    reify {
      val _cb = cb.splice
      val v = try {
        Some (f.splice)
      } catch {
        case t: Throwable =>
          _cb.fail (t)
          None
      }
      if (v.isDefined)
        _cb.pass (v.get)
    }}

  def invoke [A] (cb: Callback [A]) (f: A): Unit =
    macro _invoke1 [A]
}
