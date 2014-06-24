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

import scala.util.Random

import com.treode.async.{Async, Backoff, Callback, Fiber, Scheduler}
import com.treode.async.implicits._
import com.treode.pickle.Picklers

import Async.{async, supply}
import Callback.ignore

object Echo {

  private val echo = {
    import Picklers._
    RequestDescriptor (0xA8L, string, string)
  }

  def attach (localId: HostId) (implicit random: Random, scheduler: Scheduler, cluster: Cluster) {

    val fiber = new Fiber
    val backoff = Backoff (100, 200)
    val period = 10000
    var start = 0L
    var count = 0

    echo.listen { case (s, from) =>
      supply (s)
    }

    class Loop (cb: Callback [Unit]) {

      val hosts = ReplyTracker.settled (0, 1, 2)

      val port = echo.open { (_, from) =>
        got (from)
      }

      val timer = cb.ensure {
        port.close()
        count += 1
        if (count % period == 0) {
          val end = System.currentTimeMillis
          val ms = (end - start) .toDouble / period.toDouble
          val qps = period.toDouble / (end - start) .toDouble * 1000.0
          println ("%8d: %10.3f ms, %10.0f qps" format (count, ms, qps))
          start = System.currentTimeMillis
        }
      } .timeout (fiber, backoff) {
        echo ("Hello World") (hosts, port)
      }

      timer.rouse()

      def got (from: Peer): Unit = fiber.execute {
        hosts += from
        if (hosts.quorum)
          timer.pass()
      }}

    def loop: Async [Unit] = async (new Loop (_))

    if (localId == HostId (2)) {
      start = System.currentTimeMillis
      scheduler.whilst (true) (loop) run (ignore)
    }}}
