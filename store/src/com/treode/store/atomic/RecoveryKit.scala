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

import com.treode.async.{Async, Latch, Scheduler}
import com.treode.async.misc.materialize
import com.treode.cluster.Cluster
import com.treode.disk.Disk
import com.treode.store.{Cohort, Library, Store, TxClock, TxId}
import com.treode.store.paxos.Paxos
import com.treode.store.tier.TierMedic

import Async.supply
import JavaConversions._
import TimedStore.{receive, checkpoint}
import WriteDeputy.{aborted, committed, preparing, preparingV0}

private class RecoveryKit (implicit
    val random: Random,
    val scheduler: Scheduler,
    val library: Library,
    val recovery: Disk.Recovery,
    val config: Store.Config
) extends Atomic.Recovery {

  val tables = new TimedMedic (this)
  val writers = newWriterMedicsMap

  def get (xid: TxId): Medic = {
    var m0 = writers.get (xid)
    if (m0 != null) return m0
    val m1 = new Medic (xid, this)
    m0 = writers.putIfAbsent (xid, m1)
    if (m0 != null) return m0
    m1
  }

  preparingV0.replay { case (xid, ops) =>
    get (xid) .preparing (TxClock.MinValue, ops)
  }

  preparing.replay { case (xid, ct, ops) =>
    get (xid) .preparing (ct, ops)
  }

  committed.replay { case (xid, gens, wt) =>
    get (xid) .committed (gens, wt)
  }

  aborted.replay { xid =>
    get (xid) .aborted()
  }

  receive.replay { case (tab, gen, novel) =>
    tables.receive (tab, gen, novel)
  }

  checkpoint.replay { case (tab, meta) =>
    tables.checkpoint (tab, meta)
  }

  def launch (implicit launch: Disk.Launch, cluster: Cluster, paxos: Paxos): Async [Atomic] =
    supply {
      import launch.disk

      val kit = new AtomicKit()
      kit.tables.recover (tables.close())
      kit.writers.recover (writers.values)
      kit.reader.attach()
      kit.writers.attach()
      kit.scanner.attach()
      kit
    }}
