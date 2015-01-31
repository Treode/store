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

package com.treode.cluster

import java.net.{SocketAddress, InetSocketAddress}
import java.util.concurrent.TimeoutException
import scala.util.Random
import scala.language.postfixOps

import com.treode.async.{Async, Backoff, Callback, Fiber, Scheduler}, 
  Async.{async, supply}, Callback.{ignore => disregard}
import com.treode.async.implicits._
import com.treode.async.stubs.{AsyncChecks, StubGlobals, StubScheduler}, StubGlobals.scheduler
import com.treode.pickle.Picklers
import org.scalatest.FlatSpec

class ClusterLiveSpec extends FlatSpec {

  implicit val random = Random
  implicit val config = Cluster.Config.suggested

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
        timer.pass (())
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
    val h1 = new Host (Cell1, Host1, 6193)
    val h2 = new Host (Cell1, Host2, 6194)
    h1.hail (h2)
    h2.hail (h1)
    converse (h1, h2)
    h1.shutdown() .await()
    h2.shutdown() .await()
  }

  it should "broadcast its listening address" in {
    val h1 = new Host (Cell1, Host1, 7432)
    val h2 = new Host (Cell1, Host2, 7433)
    h1.hail (h2)
    scheduler.run (h2.address (Host1) == null)
    h1.shutdown() .await()
    h2.shutdown() .await()
  }

  it should "reject foreign cells" in {
    val h1 = new Host (Cell1, Host1, 3023)
    val h2 = new Host (Cell2, Host2, 3024)
    h1.hail (h2)
    intercept [TimeoutException] {
      h1.send (0, h2) .await()
    }
    h1.shutdown() .await()
    h2.shutdown() .await()
  }}
