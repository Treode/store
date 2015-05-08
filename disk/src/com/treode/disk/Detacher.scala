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

package com.treode.disk

import com.treode.async.{Async, Callback}, Async.async
import com.treode.async.implicits._

import Detacher._

/** Assist the LogWriter and PageWWriter in beginning a drain.
  *
  * When the user wants to drain a disk drive, its writers may be either enqueued in a dispatcher
  * awaiting a batch of items, or serializing and flushing a batch. In the first case, the writer
  * can detach immediately but must return the batch when it finally receives one from the
  * dispatcher. In the second case, the writer cannot detach until after it completes its flush.
  * The detacher tracks that writer's status and coordinates starting a drain.
  *
  * @param draining Should the detacher start already detached?
  */
private class Detacher (draining: Boolean) {

  private var state: State =
    if (draining) Detached else Waiting

  /** The user wants to start draining the disk drive.
    * @return When the writer will not write any more items.
    */
  def startDraining(): Async [Unit] =
    async { cb =>
      synchronized {
        state match {
          case Waiting =>
            state = Detached
            cb.pass (())
          case Flushing =>
            state = Detaching (cb)
          case _ =>
            // We do not expect to start a drain twice.
            throw new IllegalStateException
        }}}

  /** The writer has received items and it might begin serializing and flushing them.
    * @return true if the writer can write the items, false if it should return them.
    */
  def startFlush(): Boolean =
    synchronized {
      state match {
        case Waiting =>
          state = Flushing
          true
        case  Detached =>
          false
        case _ =>
          // We do not expect to start a flush when already flushing.
          throw new IllegalStateException
      }}

  /** The writer has finished serializing and flushing items, and it might enqueue itself to for
    * another batch.
    * @return true if the writer can await another batch, false if it should not.
    */
  def finishFlush(): Boolean =
    synchronized {
      state match {
        case Flushing =>
          state = Waiting
          true
        case Detaching (cb) =>
          state = Detached
          cb.pass (())
          false
        case _ =>
          // We do not expect to finish a flush when not flushing.
          throw new IllegalStateException
      }}}

private object Detacher {

  sealed abstract class State

  /** The writer is enqueued with a dispatcher and waiting to receive a batch of records or pages.
    */
  case object Waiting extends State

  /** The writer is serializing and flushing a batch; we have _not_ received a request to drain.
    */
  case object Flushing extends State

  /** The writer is serializing and flushing a batch; we _have_ received a request to start
    * draining.
    */
  case class Detaching (cb: Callback [Unit]) extends State

  /** The writer is not enqueued with a dispatcher, nor is it serializing and flushing a batch.
    */
  case object Detached extends State
}