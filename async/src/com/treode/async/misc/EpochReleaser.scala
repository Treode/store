/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode.async.misc

import scala.collection.mutable.Builder
import scala.util.Success

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.async.implicits._

import Async.async
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

  def release(): Async [Unit] =
    async (cb => release (toRunnable (cb, Success (()))))

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
