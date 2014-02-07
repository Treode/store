package com.treode

import scala.language.experimental.macros
import scala.reflect.macros.Context

package object async {

  def _continue [A: c.WeakTypeTag, B: c.WeakTypeTag]
      (c: Context) (cb: c.Expr [Callback [A]]) (f: c.Expr [B => Any]): c.Expr [Callback [B]] = {
    import c.universe._
    reify {
      new Callback [B] {
        private val _cb = cb.splice
        def pass (v: B) {
          try {
            f.splice.apply (v)
          } catch {
            case t: Throwable =>
              _cb.fail (t)
          }}
        def fail (t: Throwable): Unit =
          _cb.fail (t)
      }}}

  def continue [A, B] (cb: Callback [A]) (f: B => Any): Callback [B] = macro _continue [A, B]

  def _callback1 [A: c.WeakTypeTag]
      (c: Context) (f: c.Expr [A => Any]): c.Expr [Callback [A]] = {
    import c.universe._
    reify {
      new Callback [A] {
        def pass (v: A): Unit = f.splice (v)
        def fail (t: Throwable): Unit = throw t
      }}}

  def callback [A] (f: A => Any): Callback [A] = macro _callback1 [A]

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
          _cb.apply (a)
        }
        def fail (t: Throwable): Unit =
          _cb.fail (t)
      }}}

  def callback [A, B] (cb: Callback [A]) (f: B => A): Callback [B] = macro _callback2 [A, B]

  def _defer [A: c.WeakTypeTag]
      (c: Context) (cb: c.Expr [Callback [A]]) (f: c.Expr [Any]): c.Expr [Unit] = {
    import c.universe._
    reify {
      try {
        f.splice
      } catch {
        case t: Throwable =>
          cb.splice.fail (t)
      }}}

  def defer [A] (cb: Callback [A]) (f: Any): Unit = macro _defer [A]

  def _invoke [A: c.WeakTypeTag]
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
        _cb (v.get)
    }}

  def invoke [A] (cb: Callback [A]) (f: A): Unit = macro _invoke [A]

  def toRunnable (task: => Any): Runnable =
    new Runnable {
      def run() = task
    }

  def toRunnable [A] (cb: Callback [A], v: A): Runnable =
    new Runnable {
      def run() = cb (v)
    }

  def toRunnable [A] (cb: Callback [A], t: Throwable): Runnable =
    new Runnable {
      def run() = cb.fail (t)
    }}
