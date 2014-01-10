package com.treode.cluster

import java.util.concurrent.{ConcurrentHashMap, Executors}
import scala.collection.JavaConversions._
import scala.util.Random
import com.treode.async.StubScheduler
import com.treode.pickle.Pickler

class StubCluster (val random: Random, val scheduler: StubScheduler) {

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

  def locate (id: Int): Acknowledgements =
    Acknowledgements.settled (hosts.keys.toSeq: _*)

  def deliver [M] (p: Pickler [M], from: HostId, to: HostId, mbx: MailboxId, msg: M) {
    if (messageFlakiness != 0.0 && random.nextDouble < messageFlakiness)
      return
    val h = hosts.get (to)
    require (h != null, s"Host $to is not installed.")
    if (messageTrace)
      println (s"$from->$to:$mbx: $msg")
    h.deliver (p, from, mbx, msg)
  }

  def runTasks (timers: Boolean = false): Unit =
    scheduler.runTasks (timers)
}

object StubCluster {

  def apply (seed: Long = 0, multithreaded: Boolean = false): StubCluster = {

    val random = new Random (seed)

    val scheduler =
      if (multithreaded)
        StubScheduler.multithreaded (Executors.newScheduledThreadPool (8))
      else
        StubScheduler.random (random)

    new StubCluster (random, scheduler)
  }}
