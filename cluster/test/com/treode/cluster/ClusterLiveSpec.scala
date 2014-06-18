package com.treode.cluster

import java.net.{SocketAddress, InetSocketAddress}
import java.util.concurrent.TimeoutException
import scala.util.Random
import scala.language.postfixOps

import com.treode.async.{Async, Backoff, Callback, Fiber, Scheduler}
import com.treode.async.implicits._
import com.treode.async.stubs.{AsyncChecks, StubScheduler}
import com.treode.pickle.Picklers
import org.scalatest.FlatSpec

import Async.{async, supply}
import Callback.{ignore => disregard}

class ClusterLiveSpec extends FlatSpec with AsyncChecks {

  implicit val random = Random
  implicit val config = ClusterConfig.suggested

  val Cell1 = CellId (0x89)
  val Cell2 = CellId (0x27)
  val Host1 = HostId (0x18)
  val Host2 = HostId (0xD9)

  val request = RequestDescriptor.apply (0xE0, Picklers.int, Picklers.unit)

  def any (port: Int) = new InetSocketAddress (port)

  def local (port: Int) = new InetSocketAddress ("localhost", port)

  class Host (val cell: CellId, val host: HostId, val port: Int) (implicit scheduler: Scheduler) {

    implicit val cluster = Cluster.live (cell, host, any (port), local (port))

    var received = Set.empty [Int]

    request.listen { (i, from) =>
      supply {
        received.synchronized (received += i)
      }
    }

    cluster.startup()

    class Loop (i: Int, to: Peer, cb: Callback [Unit]) {

      val fiber = new Fiber

      val backoff = Backoff (100, 200, 1000, 7)

      val port = request.open { (_, from) =>
        got()
      }

      val timer = cb.ensure {
        port.close()
      } .timeout (fiber, backoff) {
        request (i) (to, port)
      }
      timer.rouse()

      def got(): Unit =
        timer.pass()
    }

    def send (i: Int, to: Host): Async [Unit] =
      async (new Loop (i, cluster.peer (to.host), _))

    def address (id: HostId): SocketAddress =
      cluster.peer (id) .address

    def hail (remote: Host): Unit =
      cluster.hail (remote.host, local (remote.port))

    def shutdown(): Async [Unit] =
      cluster.shutdown()
  }

  def converse (h1: Host, h2: Host) (implicit scheduler: StubScheduler) {
    scheduler.execute {
      for (i <- 200 until 210)
        h1.send (i, h2) run (disregard)
    }
    scheduler.execute {
      for (i <- 100 until 110)
        h2.send (i, h1) run (disregard)
    }
    scheduler.run (h1.received.size < 10)
    scheduler.run (h2.received.size < 10)
    assertResult (100 until 110 toSet) (h1.received)
    assertResult (200 until 210 toSet) (h2.received)
  }

  "The live cluster" should "handle simultaneous mutual connections" in {
    multithreaded { implicit scheduler =>
      val h1 = new Host (Cell1, Host1, 6193)
      val h2 = new Host (Cell1, Host2, 6194)
      h1.hail (h2)
      h2.hail (h1)
      converse (h1, h2)
      h1.shutdown() .await()
      h2.shutdown() .await()
    }}

  it should "broadcast its listening address" in {
    multithreaded { implicit scheduler =>
      val h1 = new Host (Cell1, Host1, 7432)
      val h2 = new Host (Cell1, Host2, 7433)
      h1.hail (h2)
      scheduler.run (h2.address (Host1) == null)
      h1.shutdown() .await()
      h2.shutdown() .await()
    }}

  it should "reject foreign cells" in {
    multithreaded { implicit scheduler =>
      val h1 = new Host (Cell1, Host1, 3023)
      val h2 = new Host (Cell2, Host2, 3024)
      h1.hail (h2)
      intercept [TimeoutException] {
        h1.send (0, h2) .await()
      }
      h1.shutdown() .await()
      h2.shutdown() .await()
    }}}
