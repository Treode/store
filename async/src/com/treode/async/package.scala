package com.treode

import scala.language.experimental.macros
import scala.reflect.macros.Context

package object async {

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
        def pass (v: B): Unit = _cb.apply (f.splice.apply (v))
        def fail (t: Throwable): Unit = _cb.fail (t)
      }}}

  def callback [A, B] (cb: Callback [A]) (f: B => A): Callback [B] = macro _callback2 [A, B]

  def _delay [A: c.WeakTypeTag, B: c.WeakTypeTag]
      (c: Context) (cb: c.Expr [Callback [A]]) (f: c.Expr [B => Any]): c.Expr [Callback [B]] = {
    import c.universe._
    reify {
      new Callback [B] {
        private val _cb = cb.splice
        def pass (v: B): Unit = f.splice.apply (v)
        def fail (t: Throwable): Unit = _cb.fail (t)
      }}}

  def delay [A, B] (cb: Callback [A]) (f: B => Any): Callback [B] = macro _delay [A, B]

  def _guard [A: c.WeakTypeTag]
      (c: Context) (cb: c.Expr [Callback [A]]) (f: c.Expr [Any]): c.Expr [Unit] = {
    import c.universe._
    reify {
      try {
        f.splice
      } catch {
        case t: Throwable => cb.splice.fail (t)
      }}}

  def guard [A] (cb: Callback [A]) (f: Any): Unit = macro _guard [A]

  def toRunnable (task: => Any): Runnable =
    new Runnable {
      def run() = task
    }}
