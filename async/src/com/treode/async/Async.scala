package com.treode.async

trait Async [A] {

  def apply (cb: Callback [A])

  def map [B] (f: A => B): Async [B] = {
    val self = this
    new Async [B] {
      def apply (cb: Callback [B]) {
        self.apply (new Callback [A] {
          def pass (v: A): Unit = cb (f (v))
          def fail (t: Throwable): Unit = cb.fail (t)
        })
      }}}

  def flatMap [B] (f: A => Async [B] ): Async [B] = {
    val self = this
    new Async [B] {
      def apply (cb: Callback [B]) {
        self.apply (new Callback [A] {
          def pass (v: A): Unit = f (v) (cb)
          def fail (t: Throwable): Unit = cb.fail (t)
        })
      }}}

  def filter (p: A => Boolean): Async [A] = {
    val self = this
    new Async [A] {
      def apply (cb: Callback [A]) {
        self.apply (new Callback [A] {
          def pass (v: A): Unit = if (p (v)) cb (v)
          def fail (t: Throwable): Unit = cb.fail (t)
        })
      }}}

  def foreach (b: A => Any) {
    apply (new Callback [A] {
      def pass (v: A): Unit = b (v)
      def fail (t: Throwable): Unit = throw t
    })
  }}

object Async {

  def apply [A] (f: Callback [A] => Any): Async [A] =
    new Async [A] {
      def apply (cb: Callback [A]): Unit = f (cb)
    }

  def const [A] (v: A): Async [A] =
    new Async [A] {
      def apply (cb: Callback [A]): Unit = cb (v)
    }}
