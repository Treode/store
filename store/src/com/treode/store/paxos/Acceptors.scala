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

package com.treode.store.paxos

import com.treode.async.{Async, Latch}
import com.treode.async.implicits._
import com.treode.async.misc.materialize
import com.treode.disk.{Disk, ObjectId, PageDescriptor, PageHandler, Position, RecordDescriptor}
import com.treode.store.{Bytes, Cell, Residents, TxClock}
import com.treode.store.tier.{TierDescriptor, TierTable}

import Async.{guard, latch, supply, when}

private class Acceptors (kit: PaxosKit) extends PageHandler [Long] {
  import kit.{archive, cluster, disk, library}
  import kit.library.atlas

  val acceptors = newAcceptorsMap

  def get (key: Bytes, time: TxClock): Acceptor = {
    var a0 = acceptors.get ((key, time))
    if (a0 != null)
      return a0
    val a1 = new Acceptor (key, time, kit)
    a0 = acceptors.putIfAbsent ((key, time), a1)
    if (a0 != null) {
      a1.dispose()
      return a0
    }
    a1
  }

  def recover (key: Bytes, time: TxClock, a: Acceptor): Unit =
    acceptors.put ((key, time), a)

  def remove (key: Bytes, time: TxClock, a: Acceptor): Unit =
    acceptors.remove ((key, time), a)

  def probe (obj: ObjectId, groups: Set [Long]): Async [Set [Long]] =
    guard {
      archive.probe (groups)
    }

  def compact (obj: ObjectId, groups: Set [Long]): Async [Unit] =
    guard {
      for {
        meta <- archive.compact (groups, library.residents)
        _ <- when (meta.isDefined) (Acceptors.compact.record (meta.get))
      } yield ()
    }

  def checkpoint(): Async [Unit] =
    guard {
      for {
        _ <- materialize (acceptors.values) .latch (_.checkpoint())
        meta <- archive.checkpoint (library.residents)
        _ <- Acceptors.checkpoint.record (meta)
      } yield ()
    }

  def attach () (implicit launch: Disk.Launch) {
    import Acceptor.{ask, choose, propose}

    launch.checkpoint (checkpoint())

    Acceptors.archive.handle (this)

    ask.listen { case ((version, key, time, ballot, default), c) =>
      if (atlas.version - 1 <= version && version <= atlas.version + 1)
        get (key, time) ask (c, ballot, default)
    }

    propose.listen { case ((version, key, time, ballot, value), c) =>
      if (atlas.version - 1 <= version && version <= atlas.version + 1)
        get (key, time) propose (c, ballot, value)
    }

    choose.listen { case ((key, time, chosen), c) =>
      get (key, time) choose (chosen)
    }}}

private object Acceptors {

  val receive = {
    import PaxosPicklers._
    RecordDescriptor (0x4DCE11AA, tuple (ulong, seq (cell)))
  }

  val compact = {
    import PaxosPicklers._
    RecordDescriptor (0x26A32F6C3BB8E697L, tierCompaction)
  }

  val checkpointV0 = {
    import PaxosPicklers._
    RecordDescriptor (0x42A17DC354412E17L, tierMeta)
  }

  val checkpoint = {
    import PaxosPicklers._
    RecordDescriptor (0x6D77917031E5416CL, tierCheckpoint)
  }

  val archive = TierDescriptor (0x9F59C4262C8190E8L) { (residents, _, cell) =>
    resident (residents, cell.key, cell.time)
  }}
