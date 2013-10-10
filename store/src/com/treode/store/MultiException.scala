package com.treode.store

class MultiException extends Exception

object MultiException {

  def apply (ts: Seq [Throwable]): MultiException = {
    val e = new MultiException
    for (t <- ts)
      e.addSuppressed (t)
    e
  }}
