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

import com.treode.async.Callback
import com.treode.store.{TxClock, TxId, WriteOp}

import Callback.ignore

private class Medic (val xid: TxId, kit: RecoveryKit)  {

  var _preparing = Option.empty [Seq [WriteOp]]
  var _committed = Option.empty [TxClock]
  var _aborted = false

  def preparing (ops: Seq [WriteOp]): Unit = synchronized {
    _preparing = Some (ops)
  }

  def committed (gens: Seq [Long], wt: TxClock): Unit = synchronized {
    _committed = Some (wt)
  }

  def aborted(): Unit = synchronized {
    _aborted = true
  }

  def close (kit: AtomicKit): WriteDeputy = synchronized {
    val w = new WriteDeputy (xid, kit)
    if (_committed.isDefined)
      w.commit (_committed.get) .run (ignore)
    if (_aborted)
      w.abort() .run (ignore)
    if (_preparing.isDefined)
      w.prepare (TxClock.MinValue, _preparing.get) run (ignore)
    w
  }

  override def toString = s"WriteDeputy.Medic($xid)"
}
