package com.treode.async.misc

import scala.collection.mutable.Builder

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.async.implicits._

import Scheduler.toRunnable

class EpochReleaser {

  private class Epoch (var parties: Int, val action: Runnable)

  private var epochs = Map.empty [Int, Epoch]
  private var epoch = 0
  private var parties = 0

  private def leave (epoch: Int, builder: Builder [Runnable, _]) {
    if (epoch == this.epoch) {
      parties -= 1
    } else {
      epochs.get (epoch) match {
        case Some (past) =>
          past.parties -= 1
          if (past.parties == 0) {
            epochs -= epoch
            builder += past.action
            leave (epoch+1, builder)
          }
        case None =>
          throw new AssertionError ("Party left forgotten epoch")
      }}}

  def release (action: Runnable) {
    val now = synchronized {
      if (epochs.isEmpty && parties == 0) {
        true
      } else {
        epochs += epoch -> new Epoch (parties, action)
        epoch += 1
        parties = 1
        false
      }}
    if (now)
      action.run()
  }

  def release (action: => Any): Unit =
    release (toRunnable (action))

  def join(): Int = synchronized {
    parties += 1
    epoch
  }

  def leave (epoch: Int) {
    val actions = synchronized {
      val builder = Seq.newBuilder [Runnable]
      leave (epoch, builder)
      builder.result
    }
    actions foreach (_.run())
  }

  def join [A] (cb: Callback [A]): Callback [A] = {
    val epoch = join()
    cb.ensure (leave (epoch))
  }

  def join [A] (task: Async [A]): Async [A] =
    new Async [A] {
      def run (cb: Callback [A]): Unit =
       task.run (join (cb))
    }}
