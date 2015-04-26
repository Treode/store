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

package com.treode.store

import java.util.concurrent.ExecutorService
import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.cluster.Cluster
import com.treode.disk.{Disk, DiskLaunch, DiskRecovery}
import com.treode.store.atomic.Atomic
import com.treode.store.catalog.Catalogs
import com.treode.store.paxos.Paxos

import Async.latch

private class RecoveryKit (implicit
    random: Random,
    scheduler: Scheduler,
    recovery: DiskRecovery,
    config: StoreConfig
) extends StoreRecovery {

  implicit val library = new Library

  implicit val _catalogs = Catalogs.recover()
  val _paxos = Paxos.recover()
  val _atomic = Atomic.recover()

  def launch (implicit launch: DiskLaunch, cluster: Cluster): Async [StoreController] = {

    for {
      catalogs <- _catalogs.launch (launch, cluster)
      paxos <- _paxos.launch (launch, cluster)
      atomic <- _atomic.launch (launch, cluster, paxos)
    } yield {

      val librarian = Librarian { atlas =>
        latch (paxos.rebalance (atlas), atomic.rebalance (atlas)) .unit
      } (scheduler, cluster, catalogs, library)

      new SimpleController (cluster, launch.controller, library, librarian, catalogs, atomic)
    }}}
