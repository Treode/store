package com.treode.async

import java.util.ArrayList
import scala.collection.JavaConversions._

import Async.async

class Future [A] (implicit scheduler: Scheduler) extends Callback [A] {

  private var callbacks = new ArrayList [Callback [A]]
  private var value = null.asInstanceOf [A]
  private var thrown = null.asInstanceOf [Throwable]

  def pass (v: A): Unit = synchronized {
    require (value == null && thrown == null, "Future was already set.")
    value = v
    val callbacks = this.callbacks
    this.callbacks = null
    callbacks foreach (scheduler.pass (_, v))
  }

  def fail (t: Throwable): Unit = synchronized {
    require (value == null && thrown == null, "Future was already set.")
    thrown = t
    val callbacks = this.callbacks
    this.callbacks = null
    callbacks foreach (_.fail (t))
  }

  def get (cb: Callback [A]): Unit = synchronized {
    if (value != null)
      cb.pass (value)
    else if (thrown != null)
      cb.fail (thrown)
    else
      callbacks.add (cb)
  }

  def get(): Async [A] =
    async (get (_))

  def await(): A =
    get() .await()
}
