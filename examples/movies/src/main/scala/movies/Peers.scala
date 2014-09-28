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

package movies

import java.net.{InetAddress, InetSocketAddress, SocketAddress}

import com.fasterxml.jackson.databind.JsonNode
import com.treode.cluster.{HostId, RumorDescriptor}
import com.treode.pickle.Picklers
import com.treode.store.{Slice, Store}
import com.twitter.finagle.util.InetSocketAddressUtil
import com.twitter.finatra.{Controller => FinatraController}

import Peers.{Info, Preference}

class Peers (controller: Store.Controller) extends FinatraController {
  import controller.store

  private var peers =
    Map.empty [HostId, Info] .withDefaultValue (Info.empty)

  controller.listen (Info.rumor) { (info, peer) =>
    synchronized {
      peers += peer.id -> info
    }}

  shareInfo (controller)

  def shareInfo (controller: Store.Controller) {
    import com.twitter.finatra.config.{port => httpPort, _}

    val localHost = InetAddress.getLocalHost

    val addr =
      if (!httpPort().isEmpty) {
        val a = InetSocketAddressUtil.parseHosts (httpPort()) .head
        Some (new InetSocketAddress (localHost, a.getPort))
      } else
        None

    val sslAddr =
      if (!certificatePath().isEmpty && !keyPath().isEmpty && !sslPort().isEmpty) {
        val a = InetSocketAddressUtil.parseHosts (sslPort()) .head
        Some (new InetSocketAddress (localHost, a.getPort))
      } else
        None

    controller.spread (Info.rumor) (Info (addr, sslAddr))
  }

  def hosts (slice: Slice): Seq [Preference] = {
    for {
      (id, count) <- store.hosts (slice)
      peer = peers (id)
      if !peer.isEmpty
    } yield Preference (count, peer.addr, peer.sslAddr)
  }

  get ("/hosts") { request =>
    render.appjson (hosts (request.getSlice)) .toFuture
  }}

object Peers {

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
  }

  case class Preference (
      weight: Int,
      addr: Option [SocketAddress],
      sslAddr: Option [SocketAddress]
  )
}
