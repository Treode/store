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

import com.treode.async.Async
import com.treode.cluster.HostId
import com.treode.cluster.stubs.{StubCluster, StubNetwork}
import com.treode.disk.{DiskLaunch, DiskRecovery}
import com.treode.store.{ChildScheduler, Library, StoreConfig}
import com.treode.store.catalog.Catalogs

private class StubPaxosMedic (
  id: HostId
) (implicit
  random: Random,
  scheduler: ChildScheduler,
  recovery: DiskRecovery,
  network: StubNetwork,
  config: StoreConfig
) {

  implicit val cluster = new StubCluster (id)
  implicit val library = new Library
  implicit val catalogs = Catalogs.recover()
  implicit val paxos = Paxos.recover()

  def close (launch: DiskLaunch): Async [StubPaxosHost] =
    for {
      catalogs <- this.catalogs.launch (launch, cluster)
      paxos <- this.paxos.launch (launch, cluster) map (_.asInstanceOf [PaxosKit])
    } yield {
      new StubPaxosHost (id) (random, scheduler, cluster, launch.disk, library, catalogs, paxos)
    }}
