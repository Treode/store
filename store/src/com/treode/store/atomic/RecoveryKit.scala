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
import com.treode.async.misc.materialize
import com.treode.cluster.Cluster
import com.treode.disk.{DiskLaunch, DiskRecovery}
import com.treode.store.{Cohort, Library, Store, StoreConfig, TxClock, TxId}
import com.treode.store.paxos.Paxos
import com.treode.store.tier.TierMedic

import Async.{latch, supply}
import Callback.ignore
import JavaConversions._
import TimedStore.{checkpoint, compact, receive}
import WriteDeputy._

private class RecoveryKit (implicit
    val random: Random,
    val scheduler: Scheduler,
    val library: Library,
    val recovery: DiskRecovery,
    val config: StoreConfig
) extends Atomic.Recovery {

  val tstore = new TimedMedic (this)
  val writers = newWriterMedicsMap

  def get (xid: TxId): Medic = {
    var m0 = writers.get (xid)
    if (m0 != null) return m0
    val m1 = new Medic (xid)
    m0 = writers.putIfAbsent (xid, m1)
    if (m0 != null) return m0
    m1
  }

  deliberating.replay { case (xid, ct, ops, rsp) =>
    get (xid) .deliberating (ct, ops, rsp)
  }

  deliberatingV0.replay { case (xid, ct, ops) =>
    get (xid) .deliberating (ct, ops, WriteResponse.Failed)
  }

  deliberatingV1.replay { case (xid, ops) =>
    get (xid) .deliberating (TxClock.MinValue, ops, WriteResponse.Failed)
  }

  deliberatingV2.replay { case (xid, ct, ops) =>
    get (xid) .deliberating (ct, ops, WriteResponse.Failed)
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

  checkpoint.replay { case (tab, meta) =>
    tstore.checkpoint (tab, meta)
  }

  def launch (implicit launch: DiskLaunch, cluster: Cluster, paxos: Paxos): Async [Atomic] =
    supply {
      import launch.disk
      val medics = writers.values
      medics.foreach (_.closeCommitted (this))
      val kit = new AtomicKit()
      kit.tstore.recover (tstore.close())
      medics.foreach (_.closeDeliberating (kit))
      kit.reader.attach()
      kit.writers.attach()
      kit.scanner.attach()
      kit
    }}
