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

import java.net.SocketAddress

import com.treode.async.Async
import com.treode.cluster.{CellId, Cluster, HostId, Peer, RumorDescriptor}
import com.treode.disk.DiskController

import Async.guard

private class ExtendedController (
  cluster: Cluster,
  protected val _disk: DiskController,
  controller: StoreController
) extends StoreController with DiskController.Proxy {

  implicit val store: Store = controller.store

  def cohorts: Seq [Cohort] =
    controller.cohorts

  def cohorts_= (v: Seq [Cohort]): Unit =
    controller.cohorts = v

  def hosts (slice: Slice): Seq [Preference] =
    controller.hosts (slice)

  def announce (addr: Option [SocketAddress], sslAddr: Option [SocketAddress]): Unit =
    controller.announce (addr, sslAddr)

  def listen [C] (desc: CatalogDescriptor [C]) (f: C => Any): Unit =
    controller.listen (desc) (f)

  def issue [C] (desc: CatalogDescriptor [C]) (version: Int, cat: C): Async [Unit] =
    controller.issue (desc) (version, cat)

  def cellId: CellId =
    cluster.cellId

  def hostId: HostId =
    cluster.localId

  def hail (remoteId: HostId, remoteAddr: SocketAddress): Unit =
    cluster.hail (remoteId, remoteAddr)

  def listen [M] (desc: RumorDescriptor [M]) (f: (M, Peer) => Any): Unit =
    cluster.listen (desc) (f)

  def spread [M] (desc: RumorDescriptor [M]) (msg: M): Unit =
    cluster.spread (desc) (msg)

  def tables: Async [Seq [TableDigest]] =
    controller.tables

  def shutdown(): Async [Unit] =
    guard [Unit] {
      for {
        _ <- cluster.shutdown()
        _ <- _disk.shutdown()
      } yield ()
    }}
