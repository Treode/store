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
