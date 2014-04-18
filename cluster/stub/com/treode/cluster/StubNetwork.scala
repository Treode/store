package com.treode.cluster

import java.util.concurrent.{ConcurrentHashMap, Executors}
import scala.collection.JavaConversions._
import scala.util.Random
import com.treode.async.StubScheduler
import com.treode.pickle.Pickler

class StubNetwork (implicit val random: Random, val scheduler: StubScheduler) {

  private val hosts = new ConcurrentHashMap [HostId, StubHost]

  var messageFlakiness = 0.0
  var messageTrace = false

  private def install (host: StubHost): Unit =
    require (
        hosts.putIfAbsent (host.localId, host) == null,
        s"Host ${host.localId} is already installed.")

  def install [H <: StubHost] (n: Int, mk: HostId => H): Seq [H] = {
    val hs = Seq.fill (n) (mk (HostId (random.nextLong)))
    hs foreach (install _)
    hs
  }

  def remove (host: HostId): Unit =
    hosts.remove (host)

  def remove (host: StubHost): Unit =
    require (
        hosts.remove (host.localId, host),
        s"Host ${host.localId} could not be removed.")

  def deliver [M] (p: Pickler [M], from: HostId, to: HostId, port: PortId, msg: M) {
    if (messageFlakiness != 0.0 && random.nextDouble < messageFlakiness)
      return
    val h = hosts.get (to)
    require (h != null, s"Host $to is not installed.")
    if (messageTrace)
      println (s"$from->$to:$port: $msg")
    h.deliver (p, from, port, msg)
  }

  def runTasks (timers: Boolean = false, count: Int = Int.MaxValue): Unit =
    scheduler.runTasks (timers, count)
}

object StubNetwork {

  def apply (random: Random, scheduler: StubScheduler): StubNetwork =
    new StubNetwork () (random, scheduler)

  def apply (random: Random = new Random (0), multithreaded: Boolean = false): StubNetwork = {
    val scheduler =
      if (multithreaded)
        StubScheduler.multithreaded (Executors.newScheduledThreadPool (8))
      else
        StubScheduler.random (random)
    new StubNetwork () (random, scheduler)
  }}
