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

import com.treode.async.Async
import com.treode.async.implicits._
import com.treode.async.misc.materialize
import com.treode.cluster.IgnoreRequestException
import com.treode.disk.Disk
import com.treode.store.TxId

import Async.guard

private class WriteDeputies (kit: AtomicKit) {
  import WriteDeputy._
  import kit.{cluster, tstore}
  import kit.library.atlas

  val deputies = newWritersMap

  def get (xid: TxId): WriteDeputy = {
    var d0 = deputies.get (xid.id)
    if (d0 != null)
      return d0
    val d1 = new WriteDeputy (xid, kit)
    d0 = deputies.putIfAbsent (xid, d1)
    if (d0 != null)
      return d0
    d1
  }

  def recover (xid: TxId, w: WriteDeputy): Unit =
    deputies.put (xid, w)

  def remove (xid: TxId, w: WriteDeputy): Unit =
    deputies.remove (xid, w)

  def checkpoint(): Async [Unit] =
    guard {
      for {
        _ <- materialize (deputies.values) .latch (_.checkpoint())
        _ <- tstore.checkpoint()
      } yield ()
    }

  def attach () (implicit launch: Disk.Launch) {

    launch.checkpoint (checkpoint())

    TimedStore.table.handle (tstore)

    prepare.listen { case ((version, xid, ct, ops), from) =>
      if (atlas.version - 1 <= version && version <= atlas.version + 1)
        get (xid) .prepare (ct, ops)
      else
        throw new IgnoreRequestException
    }

    commit.listen { case ((xid, wt), from) =>
      get (xid) .commit (wt)
    }

    abort.listen { case (xid, from) =>
      get (xid) .abort()
    }}}
