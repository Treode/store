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

import scala.collection.JavaConversions
import scala.util.Random

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.async.implicits._
import com.treode.async.misc.materialize
import com.treode.cluster.Cluster
import com.treode.disk.Disk
import com.treode.store.{Cohort, Library, Store, TxClock, TxId}
import com.treode.store.paxos.Paxos
import com.treode.store.tier.TierMedic

import Async.{latch, supply}
import Callback.ignore
import JavaConversions._
import TimedStore.{checkpoint, checkpointV0, compact, receive}
import WriteDeputy._

private class RecoveryKit (implicit
    val random: Random,
    val scheduler: Scheduler,
    val library: Library,
    val recovery: Disk.Recovery,
    val config: Store.Config
) extends Atomic.Recovery {

  val tstore = new TimedMedic (this)
  val writers = newWriterMedicsMap

  def get (xid: TxId): Medic = {
    var m0 = writers.get (xid)
    if (m0 != null) return m0
    val m1 = new Medic (xid, this)
    m0 = writers.putIfAbsent (xid, m1)
    if (m0 != null) return m0
    m1
  }

  preparing.replay { case (xid, ct, ops) =>
    get (xid) .preparing (ct, ops)
  }

  preparedV0.replay { case (xid, ops) =>
    get (xid) .prepared (TxClock.MinValue, ops)
  }

  prepared.replay { case (xid, ct, ops) =>
    get (xid) .prepared (ct, ops)
  }

  committed.replay { case (xid, gens, wt) =>
    get (xid) .committed (gens, wt)
  }

  aborted.replay { xid =>
    get (xid) .aborted()
  }

  receive.replay { case (tab, gen, novel) =>
    tstore.receive (tab, gen, novel)
  }

  compact.replay { case (tab, meta) =>
    tstore.compact (tab, meta)
  }

  checkpointV0.replay { case (tab, meta) =>
    tstore.checkpoint (tab, meta)
  }

  checkpoint.replay { case (tab, meta) =>
    tstore.checkpoint (tab, meta)
  }

  def launch (implicit launch: Disk.Launch, cluster: Cluster, paxos: Paxos): Async [Atomic] = {
    import launch.disk
    val kit = new AtomicKit()
    kit.tstore.recover (tstore.close())
    val medics = writers.values
    for {
      _ <- medics.filter (_.isCommitted) .latch (_.close (kit))
    } yield {
      medics.filter (_.isPrepared) .foreach (_.close (kit) .run (ignore))
      kit.reader.attach()
      kit.writers.attach()
      kit.scanner.attach()
      kit
    }}}
