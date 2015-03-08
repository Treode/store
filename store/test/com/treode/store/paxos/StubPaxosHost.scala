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

import com.treode.async.{Async, BatchIterator, Scheduler}
import com.treode.async.io.stubs.StubFile
import com.treode.cluster.{Cluster, HostId}
import com.treode.cluster.stubs.{StubCluster, StubNetwork}
import com.treode.disk.Disk
import com.treode.disk.stubs.{StubDisk, StubDiskDrive}
import com.treode.store._
import com.treode.store.catalog.Catalogs
import com.treode.store.tier.TierTable
import org.scalatest.Assertions

import Async.{guard, supply}
import StoreTestTools._

private class StubPaxosHost (
    val localId: HostId
) (implicit
    val random: Random,
    val scheduler: ChildScheduler,
    val cluster: StubCluster,
    val disk: Disk,
    val library: Library,
    val catalogs: Catalogs,
    val paxos: PaxosKit
) extends StoreClusterChecks.Host {

  val librarian = Librarian (paxos.rebalance _)

  cluster.startup()

  def shutdown(): Async [Unit] =
    for {
      _ <- cluster.shutdown()
    } yield {
      scheduler.shutdown()
    }

  def setAtlas (cohorts: Cohort*) {
    val atlas = Atlas (cohorts.toArray, 1)
    library.atlas = atlas
    library.residents = atlas.residents (localId)
  }

  def issueAtlas (cohorts: Cohort*): Unit =
    librarian.issueAtlas (cohorts.toArray)

  def atlas: Atlas =
    library.atlas

  def unsettled: Boolean =
    !library.atlas.settled

  def acceptorsOpen: Boolean =
    !paxos.acceptors.acceptors.isEmpty

  def proposersOpen: Boolean =
    !paxos.proposers.proposers.isEmpty

  def audit: BatchIterator [Cell] =
    paxos.archive.iterator (Residents.all)

  def lead (key: Long, value: Int): Async [Int] =
    paxos.lead (Bytes (key), 0, Bytes (value)) .map (_.int)

  def propose (key: Long, value: Int): Async [Int] =
    paxos.propose (Bytes (key), 0, Bytes (value)) .map (_.int)
}

private object StubPaxosHost extends StoreClusterChecks.Package [StubPaxosHost] {

  def boot (
      id: HostId,
      drive: StubDiskDrive
  ) (implicit
      random: Random,
      parent: Scheduler,
      network: StubNetwork,
      config: StoreTestConfig
  ): Async [StubPaxosHost] = {

    import config._

    implicit val scheduler = new ChildScheduler (parent)
    implicit val cluster = new StubCluster (id)
    implicit val library = new Library
    implicit val recovery = StubDisk.recover()
    implicit val _catalogs = Catalogs.recover()
    val _paxos = Paxos.recover()

    for {
      launch <- recovery.reattach (drive)
      catalogs <- _catalogs.launch (launch, cluster)
      paxos <- _paxos.launch (launch, cluster) map (_.asInstanceOf [PaxosKit])
    } yield {
      launch.launch()
      new StubPaxosHost (id) (random, scheduler, cluster, launch.disk, library, catalogs, paxos)
    }}

  def install () (implicit r: Random, s: Scheduler, n: StubNetwork): Async [StubPaxosHost] = {
    implicit val config = StoreTestConfig()
    boot (r.nextLong, new StubDiskDrive)
  }}
