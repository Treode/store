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

import com.treode.cluster.{Cluster, HostId, RumorDescriptor}
import com.treode.pickle.Picklers

import PeersScuttlebutt.Info

private class PeersScuttlebutt (cluster: Cluster, atlas: Atlas) {

  private var peers =
    Map.empty [HostId, Info] .withDefaultValue (Info.empty)

  cluster.listen (Info.rumor) { (info, peer) =>
    synchronized {
      peers += peer.id -> info
    }}

  def spread (addr: Option [SocketAddress], sslAddr: Option [SocketAddress]): Unit =
    cluster.spread (Info.rumor) (Info (addr, sslAddr))

  def hosts (slice: Slice): Seq [Preference] = {
    for {
      (id, count) <- atlas.hosts (slice)
      peer = peers (id)
      if !peer.isEmpty
    } yield Preference (count, id, peer.addr, peer.sslAddr)
  }}

private object PeersScuttlebutt {

  case class Info (addr: Option [SocketAddress], sslAddr: Option [SocketAddress]) {

    def isEmpty: Boolean =
      addr.isEmpty && sslAddr.isEmpty
  }

  object Info {

    val empty = Info (None, None)

    val pickler = {
      import Picklers._
      wrap (option (socketAddress), option (socketAddress))
      .build (v => Info (v._1, v._2))
      .inspect (v => (v.addr, v.sslAddr))
    }

    val rumor = RumorDescriptor (0x0E61B91E0E732D59L, pickler)
  }}
