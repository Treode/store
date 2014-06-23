package example

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
