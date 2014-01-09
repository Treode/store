package com.treode.async

private class FilteredIterator [A] private (iter: AsyncIterator [A], pred: A => Boolean)
extends AsyncIterator [A] {

  private var next = null.asInstanceOf [A]

  private class Looper [B] (cb: Callback [B], v: B) extends Callback [A] {

    def pass (x: A) {
      if (pred (x)) {
        next = x
        cb (v)
      } else if (iter.hasNext) {
        iter.next (this)
      } else {
        next = null.asInstanceOf [A]
        cb (v)
      }}

    def fail (t: Throwable) {
      next = null.asInstanceOf [A]
      cb.fail (t)
    }}

  private def init (cb: Callback [AsyncIterator [A]]): Unit =
    if (iter.hasNext)
      iter.next (new Looper (cb, this))
    else
      cb (this)

  def hasNext: Boolean = next != null

  def next (cb: Callback [A]) {
    if (iter.hasNext)
      iter.next (new Looper (cb, next))
    else {
      val t = next
      next = null.asInstanceOf [A]
      cb (t)
    }}}

private object FilteredIterator {

  def apply [A] (iter: AsyncIterator [A], pred: A => Boolean, cb: Callback [AsyncIterator [A]]): Unit =
    new FilteredIterator (iter, pred) .init (cb)
}
