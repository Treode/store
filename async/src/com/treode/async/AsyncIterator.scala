package com.treode.async

trait AsyncIterator [+A] {

  def hasNext: Boolean
  def next (cb: Callback [A])
}

object AsyncIterator {

  /** Transform the standard iterator into an AsyncIterator. */
  def adapt [A] (iter: Iterator [A]): AsyncIterator [A] =
    new AsyncIterator [A] {
      def hasNext: Boolean = iter.hasNext
      def next (cb: Callback [A]): Unit = cb (iter.next)
    }

  /** Transform the standard iterable into an AsyncIterator. */
  def adapt [A] (iter: Iterable [A]): AsyncIterator [A] =
    adapt (iter.iterator)

  private class Scanner [A] (iter: AsyncIterator [A], cb: Callback [Seq [A]])
  extends Callback [A] {

    val builder = Seq.newBuilder [A]

    def pass (x: A) {
      builder += x
      if (iter.hasNext)
        iter.next (this)
      else
        cb (builder.result)
    }

    def fail (t: Throwable) = cb.fail (t)
  }

  /** Iterator the entire asynchronous iterator and build a standard sequence. */
  def scan [A] (iter: AsyncIterator [A], cb: Callback [Seq [A]]) {
    if (iter.hasNext)
      iter.next (new Scanner (iter, cb))
    else
      cb (Seq.empty)
  }

  def filter [A] (iter: AsyncIterator [A], pred: A => Boolean, cb: Callback [AsyncIterator [A]]): Unit =
    FilteredIterator (iter, pred, cb)

  /** Given asynchronous iterators of sorted items, merge them into single asynchronous iterator
    * that maintains the sort.  Keep duplicate elements, and when two or more input iterators
    * duplicate an element, first list the element from the earlier iterator (that is, by position
    * in `iters`).
    */
  def merge [A] (iters: Iterator [AsyncIterator [A]], cb: Callback [AsyncIterator [A]]) (
      implicit ordering: Ordering [A]): Unit =
    MergeIterator (iters, cb)
}
