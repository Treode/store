package com.treode.async

import java.util.ArrayList
import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

import Async.async

/** An asynchronous future. */
class Future [A] extends Async [A] with Callback [A] {

  private var callbacks = new ArrayList [Callback [A]]
  private var value: Try [A] = null

  def apply (v: Try [A]): Unit = synchronized {
    require (value == null, "Future was already set.")
    value = v
    val callbacks = this.callbacks
    this.callbacks = null
    callbacks foreach (_ (v))
  }

  def run (cb: Callback [A]): Unit = synchronized {
    if (value != null)
      cb (value)
    else
      callbacks.add (cb)
  }}
