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
import java.nio.file.Path
import java.util.concurrent.ExecutorService

import com.treode.async.Async
import com.treode.cluster.{CellId, Cluster, HostId, Peer, RumorDescriptor}
import com.treode.disk.{Disk, DiskController, DriveAttachment, DriveDigest, DriveGeometry}
import com.treode.store.atomic.Atomic
import com.treode.store.catalog.Catalogs

import Async.supply

private class SimpleController (
    cluster: Cluster,
    disk: DiskController,
    library: Library,
    librarian: Librarian,
    catalogs: Catalogs,
    atomic: Atomic
) extends StoreController {

  private val peers =
    new PeersScuttlebutt (cluster, library.atlas)

  def store: Store =
    atomic

  def cohorts: Seq [Cohort] =
    library.atlas.cohorts.toSeq

  def cohorts_= (v: Seq [Cohort]): Unit =
    librarian.issueAtlas (v.toArray)

  def hosts (slice: Slice): Seq [Preference] =
    peers.hosts (slice)

  def announce (addr: Option [SocketAddress], sslAddr: Option [SocketAddress]): Unit =
    peers.spread (addr, sslAddr)

  def listen [C] (desc: CatalogDescriptor [C]) (f: C => Any): Unit =
    catalogs.listen (desc) (f)

  def issue [C] (desc: CatalogDescriptor [C]) (version: Int, cat: C): Async [Unit] =
    catalogs.issue (desc) (version, cat)

  def drives: Async [Seq [DriveDigest]] =
    disk.drives

  def attach (items: DriveAttachment*): Async [Unit] =
    disk.attach (items: _*)

  def drain (paths: Path*): Async [Unit] =
    disk.drain (paths: _*)

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
    atomic.tables

  def shutdown(): Async [Unit] =
    supply (())
}
