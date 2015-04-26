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

/** The EpochReleaser prevents objects from being reclaimed while short-lived readers may hold
  * references to them. A short-lived reader joins an epoch to protect objects it will be using.
  * When a mutator releases old references, they are reclaimed after all readers of the epoch have
  * left it.
  *
  * Levandoski, Justin J. and Lomet, David B. and Sengupta, Sudipta. "The BW-Tree: A B-tree for New
  * Hardware Platforms." Proceedings of the 2013 IEEE International Conference on Data Engineering
  * (ICDE 2013).
  */
class EpochReleaser {

  /** A past epoch that still has parties, and actions awaiting their departure. */
  private class Epoch (var parties: Int, val action: Runnable)

  /** Past epochs. */
  private var epochs = Map.empty [Int, Epoch]

  /** The current epoch; advanced by one when a new action is registered. */
  private var epoch = 0

  /** The number of parties that have joined the current epoch. */
  private var parties = 0

  /** Leave the given epoch, building a list of actions to perform now. */
  private def leave (epoch: Int, builder: Builder [Runnable, _]) {
    if (epoch == this.epoch) {
      // Leave the current epoch.
      parties -= 1
    } else {
      // Leave a past epoch.
      epochs.get (epoch) match {
        case Some (past) =>
          // Leave the past epoch.
          past.parties -= 1
          if (past.parties == 0) {
            // If this is the last party to the epoch, perform its actions. Also, leave the next
            // epoch, because epoch N includes epoch N-1 as a party.
            epochs -= epoch
            builder += past.action
            leave (epoch+1, builder)
          }
        case None =>
          throw new AssertionError ("Party left forgotten epoch")
      }}}

  /** Register an action to perform when all parties have left the current epoch.
    * @param action The action to perform.
    */
  def release (action: Runnable) {
    val now = synchronized {
      if (epochs.isEmpty && parties == 0) {
        // There are no parties in the current epoch. Run the action now.
        true
      } else {
        // There are parties in the current epoch. Queue the action for later.
        epochs += epoch -> new Epoch (parties, action)
        epoch += 1
        parties = 1
        false
      }}
    if (now)
      action.run()
  }

  /** Register an action to perform when all parties have left the current epoch.
    * @param action The action to perform.
    */
  def release (action: => Any): Unit =
    release (toRunnable (action))

  /** Wait until all parties have left the current epoch.
    * @return Unit when all current parties have left the epoch.
    */
  def release(): Async [Unit] =
    async (cb => release (toRunnable (cb, Success (()))))

  /** Join an epoch. Objects released before a call to join are not visible to the party now
    * joining. Objects released after a call to join will not be reclaimed until a matching call
    * to `leave`.
    *
    * @return The epoch number to feed to the matching call to `leave`.
    */
  def join(): Int = synchronized {
    parties += 1
    epoch
  }

  /** Leave an epoch. If this is the last party to leave the epoch, then all actions registered
    * during that epoch will be reclaimed.
    * @param epoch The epoch number from `join`.
    */
  def leave (epoch: Int) {
    val actions = synchronized {
      val builder = Seq.newBuilder [Runnable]
      leave (epoch, builder)
      builder.result
    }
    actions foreach (_.run())
  }

  /** Join and automatically leave an epoch. Join the current epoch, and leave it when the callback
    * is invoked.
    * @param cb The callback to continue after leaving the epoch.
    * @return A new callback that leaves the epoch before invoking the given one.
    */
  def join [A] (cb: Callback [A]): Callback [A] = {
    val epoch = join()
    cb.ensure (leave (epoch))
  }

  /** Join and automatically leave an epoch. Join the current epoch, and leave it when the
    * asynchronous operation completes.
    * @param task The asynchronous operation to await before leaving the epoch.
    */
  def join [A] (task: Async [A]): Async [A] =
    new Async [A] {
      def run (cb: Callback [A]): Unit =
       task.run (join (cb))
    }}
