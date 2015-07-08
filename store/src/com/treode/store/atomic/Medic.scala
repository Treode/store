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

import com.treode.store.{TxClock, TxId, WriteOp}

import Medic._

private class Medic (val xid: TxId)  {

  var _deliberating = Option.empty [Deliberating]
  var _committed = Option.empty [Committed]
  var _aborted = false

  def deliberating (ct: TxClock, ops: Seq [WriteOp], rsp: WriteResponse): Unit = synchronized {
    _deliberating = Some (Deliberating (ct, ops, rsp))
  }

  def committed (gens: Seq [Long], wt: TxClock): Unit = synchronized {
    _committed = Some (Committed (gens, wt))
  }

  def aborted(): Unit = synchronized {
    _aborted = true
  }

  def closeCommitted (kit: RecoveryKit): Unit = synchronized {
    assert (_committed.isEmpty || !_aborted)
    if (_deliberating.isDefined && _committed.isDefined) {
      val Committed (gens, wt) = _committed.get
      val Deliberating (ct, ops, rsp) = _deliberating.get
      kit.tstore.commit (gens, wt, ops)
    }}

  def closeDeliberating (kit: AtomicKit): Unit = synchronized {
    if (_deliberating.isDefined && _committed.isEmpty && !_aborted) {
      val w = new WriteDeputy (xid, kit)
      val Deliberating (ct, ops, rsp) = _deliberating.get
      w.recover (ct, ops, rsp)
      kit.writers.recover (xid, w)
    }}

  override def toString =
    s"WriteDeputy.Medic(${xid.toShortString}, ${_deliberating}, ${_committed}, ${_aborted})"
}

private object Medic {

  case class Deliberating (ct: TxClock, ops: Seq [WriteOp], rsp: WriteResponse)
  case class Committed (gens: Seq [Long], wt: TxClock)
}
