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

import java.util.concurrent.Executors
import scala.util.Random

import com.treode.async.stubs.StubScheduler
import com.treode.cluster.HostId
import com.treode.cluster.stubs.StubNetwork

class StoreTestKit private (implicit
    val random: Random,
    val scheduler: StubScheduler,
    val network: StubNetwork) {

  def messageFlakiness: Double =
    network.messageFlakiness

  def messageFlakiness_= (v: Double): Unit =
    network.messageFlakiness = v

  def run (timers: => Boolean = false, count: Int = Int.MaxValue): Int =
    scheduler.run (timers, count)
}

object StoreTestKit {

  def random(): StoreTestKit =
    random (new Random (0))

  def random (random: Random): StoreTestKit = {
    val scheduler = StubScheduler.random (random)
    val network = StubNetwork (random)
    new StoreTestKit () (random, scheduler, network)
  }

  def random (random: Random, scheduler: StubScheduler): StoreTestKit = {
    val network = StubNetwork (random)
    new StoreTestKit () (random, scheduler, network)
  }

  def multithreaded (scheduler: StubScheduler): StoreTestKit = {
    val network = StubNetwork (Random)
    new StoreTestKit () (Random, scheduler, network)
  }}
