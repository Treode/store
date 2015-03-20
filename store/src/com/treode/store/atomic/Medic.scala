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

package com.treode.store.atomic

import com.treode.async.{Async, Callback}
import com.treode.store.{TxClock, TxId, WriteOp}

import Async.supply
import Callback.ignore
import Medic._

private class Medic (val xid: TxId, kit: RecoveryKit)  {

  var _state: State = Open
  var _committed = Option.empty [TxClock]
  var _aborted = false

  def preparing (ct: TxClock, ops: Seq [WriteOp]): Unit = synchronized {
    _state match {
      case Open => _state = Preparing (ct, ops)
      case _    => ()
    }}

  def prepared (ct: TxClock, ops: Seq [WriteOp]): Unit = synchronized {
    _state match {
      case Open => _state = Prepared (ct, ops)
      case Preparing (_, _) => _state = Prepared (ct, ops)
      case _ => ()
    }}

  def committed (gens: Seq [Long], wt: TxClock): Unit = synchronized {
    _committed = Some (wt)
  }

  def aborted(): Unit = synchronized {
    _aborted = true
  }

  def isPrepared: Boolean =
    _state match {
      case Prepared (_, _) => _committed.isEmpty
      case _ => false
   }

  def isCommitted: Boolean =
    _state match {
      case Open => false
      case _ => _committed.isDefined
   }

  def prepare (w: WriteDeputy): Async [Unit] =
    _state match {
      case Open =>
        supply (())
      case Prepared (ct, ops) =>
        w.prepare (ct, ops) .unit
      case Preparing (ct, ops) =>
        w.prepare (ct, ops) .unit
    }

  def close (kit: AtomicKit): Async [Unit] = {
    val w = new WriteDeputy (xid, kit)
    if (_committed.isDefined)
      w.commit (_committed.get) .run (ignore)
    if (_aborted)
      w.abort() .run (ignore)
    for {
      _ <- prepare (w)
    } yield {
      kit.writers.recover (xid, w)
    }}

  override def toString =
    s"WriteDeputy.Medic(${xid.toShortString}, ${_state}, ${_committed}, ${_aborted})"
}

private object Medic {

  sealed abstract class State
  case object Open extends State
  case class Preparing (ct: TxClock, ops: Seq [WriteOp]) extends State
  case class Prepared (ct: TxClock, ops: Seq [WriteOp]) extends State
}
