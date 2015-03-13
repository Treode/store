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

import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.cluster.{Cluster, ReplyTracker}
import com.treode.disk.Disk
import com.treode.store.{Atlas, Bytes, Cohort, Library, StoreConfig, TxClock}
import com.treode.store.tier.TierTable

import Async.{async, when}
import PaxosMover.Targets

private class PaxosKit (
    val archive: TierTable
) (implicit
    val random: Random,
    val scheduler: Scheduler,
    val cluster: Cluster,
    val disk: Disk,
    val library: Library,
    val config: StoreConfig
) extends Paxos {

  val acceptors = new Acceptors (this)
  val proposers = new Proposers (this)
  val mover = new PaxosMover (this)

  def lead (key: Bytes, time: TxClock, value: Bytes): Async [Bytes] =
    proposers.propose (0, key, time, value)

  def propose (key: Bytes, time: TxClock, value: Bytes): Async [Bytes] =
    proposers.propose (random.nextInt (17) + 1, key, time, value)

  def rebalance (atlas: Atlas): Async [Unit] = {
    val targets = Targets (atlas)
    for {
      _ <- mover.rebalance (targets)
    } yield {
      if (targets.isEmpty)
        archive.compact()
    }}}
