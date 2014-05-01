package com.treode.async

/** Collects multiple exceptions into one.  The collected exceptions can be retrieved from
  * `getSuppressed`.
  */
class MultiException extends Exception

object MultiException {

  /** Create a MultiException, even if the sequence has zero or one exceptions. */
  def apply (ts: Seq [Throwable]): MultiException = {
    val e = new MultiException
    for (t <- ts)
      e.addSuppressed (t)
    e
  }

  /** Create a MultiException unless the sequence has one exception. */
  def fit (ts: Seq [Throwable]): Throwable =
    if (ts.size == 1) ts.head else apply (ts)
}
