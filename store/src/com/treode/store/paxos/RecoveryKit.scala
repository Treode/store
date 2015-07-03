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

import scala.collection.JavaConversions
import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.async.misc.materialize
import com.treode.cluster.Cluster
import com.treode.disk.{DiskLaunch, DiskRecovery}
import com.treode.store.{Bytes, Library, StoreConfig, TxClock}
import com.treode.store.tier.TierMedic

import Async.supply
import Acceptors.{checkpoint, compact, receive}
import Acceptor.{accept, close, grant, open, reaccept}
import JavaConversions._

private class RecoveryKit (implicit
    val random: Random,
    val scheduler: Scheduler,
    val library: Library,
    val recovery: DiskRecovery,
    val config: StoreConfig
) extends Paxos.Recovery {

  val archive = TierMedic (Acceptors.archive, 0)
  val medics = newMedicsMap

  def get (key: Bytes, default: Option [Bytes]): Medic = {
    var m0 = medics.get (key)
    if (m0 != null) return m0
    val m1 = Medic (key, default, this)
    m0 = medics.putIfAbsent (key, m1)
    if (m0 != null) return m0
    return m1
  }

  open.replay { case (key, default) =>
    get (key, Some (default)) opened (default)
  }

  grant.replay { case (key, ballot) =>
    get (key, None) granted (ballot)
  }

  accept.replay { case (key, ballot, value) =>
    get (key, Some (value)) accepted (ballot, value)
  }

  reaccept.replay { case (key, ballot) =>
    get (key, None) reaccepted (ballot)
  }

  close.replay { case (key, chosen, gen) =>
    get (key, Some (chosen)) closed (chosen, gen)
  }

  receive.replay { case (gen, novel) =>
    archive.receive (gen, novel)
  }

  compact.replay { meta =>
    archive.compact (meta)
  }

  checkpoint.replay { meta =>
    archive.checkpoint (meta)
  }

  def launch (implicit launch: DiskLaunch, cluster: Cluster): Async [Paxos] =
    supply {
      import launch.disk
      val kit = new PaxosKit (archive.close())
      medics.values foreach (_.close (kit))
      kit.acceptors.attach()
      kit.proposers.attach()
      kit
    }}
