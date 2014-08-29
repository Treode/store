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
import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.async.misc._
import com.treode.pickle.Pickler
import sun.misc.{SignalHandler, Signal}

import Async.supply

class EchoTest (localId: HostId, addresses: Seq [InetSocketAddress]) {

  private val _cellId = CellId (0x9E)
  private val _config = Cluster.Config.suggested
  private val _random = Random
  private val _exiting = new AtomicBoolean (false)

  private var _executor: ScheduledExecutorService = null
  private var _scheduler: Scheduler = null
  private var _cluster: Cluster = null

  private def _fork (f: => Unit) {
    new Thread() {
      override def run(): Unit = f
    }.start()
  }

  private def _shutdown (name: String, cond: Boolean) (f: => Any) {
    if (cond) {
      println ("Shutting down " + name)
      try {
        f
      } catch {
        case e: Throwable =>
          println ("Error shutting down " + name)
          e.printStackTrace()
      }}}

  def _shutdown() {

    if (!_exiting.compareAndSet (false, true))
      return

    _shutdown ("cluster services", _cluster != null) (_cluster.shutdown())

    _shutdown ("executor", _executor != null) {
      _executor.shutdown()
      _executor.awaitTermination (2, TimeUnit.SECONDS)
    }
    _shutdown ("executor by force", _executor != null && !_executor.isTerminated) {
      _executor.shutdownNow()
    }

    println ("Bye-bye.")
    System.exit (0)
  }

  Signal.handle (new Signal ("INT"), new SignalHandler {
    def handle (s: Signal): Unit = _shutdown()
  })

  Signal.handle (new Signal ("TERM"), new SignalHandler {
    def handle (s: Signal): Unit = _shutdown()
  })

  try {

    val nt = Runtime.getRuntime.availableProcessors
    println ("Using " + nt + " threads")
    _executor = Executors.newScheduledThreadPool (nt)
    _scheduler = Scheduler (_executor)

    val localAddr = addresses (localId.id.toInt)
    val clusterConfig = Cluster.Config.suggested
    _cluster =
      Cluster.live (0xE8, localId, localAddr, localAddr) (Random, _scheduler, clusterConfig)

    Echo.attach (localId) (_random, _scheduler, _cluster)

    for ((addr, i) <- addresses.zipWithIndex)
      _cluster.hail (i, addr)
    _cluster.startup()

  } catch {
    case e: Throwable =>
      _fork (_shutdown())
      throw e
  }}

object EchoTest {

  private val usage = "usage: EchoTest p0 p1 p2 id"

  def main (args: Array[String] ) {
    (args.length == 4) orDie usage
    new EchoTest (
      parseInt (args (3)) getOrDie usage,
      Seq (
        parseInetSocketAddress (args (0)) getOrDie usage,
        parseInetSocketAddress (args (1)) getOrDie usage,
        parseInetSocketAddress (args (2)) getOrDie usage))
  }}
