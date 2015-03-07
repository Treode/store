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

import com.treode.async.Async
import com.treode.cluster.{CellId, HostId, Peer, RumorDescriptor}
import com.treode.disk.{DriveAttachment, DriveDigest}

trait StoreController {

  implicit def store: Store

  def cohorts: Seq [Cohort]

  def cohorts_= (cohorts: Seq [Cohort])

  /** The preferred hosts for a slice. See [[Slice]] and [[Preference]] for details. The
    * addresses provided in the preferences are the ones the peer announced when starting up.
    *
    * @param slice The slice to query.
    * @return The weighted preferences for this slice.
    */
  def hosts (slice: Slice): Seq [Preference]

  /** Announce the client addresses for this peer to all other peers. The addresses announced
    * here will be provided in preferences obtained from the `hosts` method.
    *
    * @param addr An address for clients to connect to this peer.
    * @param sslAddr An address for clients to connect to this peer.
    */
  def announce (addr: Option [SocketAddress], sslAddr: Option [SocketAddress])

  def listen [C] (desc: CatalogDescriptor [C]) (f: C => Any)

  def issue [C] (desc: CatalogDescriptor [C]) (version: Int, cat: C): Async [Unit]

  def drives: Async [Seq [DriveDigest]]

  def attach (items: DriveAttachment*): Async [Unit]

  def drain (paths: Path*): Async [Unit]

  def cellId: CellId

  def hostId: HostId

  def hail (remoteId: HostId, remoteAddr: SocketAddress)

  def listen [M] (desc: RumorDescriptor [M]) (f: (M, Peer) => Any)

  def spread [M] (desc: RumorDescriptor [M]) (msg: M)

  def tables: Async [Seq [TableDigest]]

  def shutdown(): Async [Unit]
}
