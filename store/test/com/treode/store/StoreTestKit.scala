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

  def runTasks (timers: Boolean = false, count: Int = Int.MaxValue): Unit =
    scheduler.runTasks (timers, count)
}

object StoreTestKit {

  def apply (random: Random): StoreTestKit = {
    val scheduler = StubScheduler.random (random)
    val network = StubNetwork (random)
    new StoreTestKit () (random, scheduler, network)
  }

  def apply(): StoreTestKit =
    apply (new Random (0))

  def multithreaded(): StoreTestKit = {
    val executor = Executors.newScheduledThreadPool (8)
    val scheduler = StubScheduler.multithreaded (executor)
    val network = StubNetwork (Random)
    new StoreTestKit () (Random, scheduler, network)
  }}
