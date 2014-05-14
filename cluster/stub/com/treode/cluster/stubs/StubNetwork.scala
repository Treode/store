package com.treode.cluster.stubs

import java.util.concurrent.{ConcurrentHashMap, Executors}
import scala.collection.JavaConversions._
import scala.util.Random

import com.treode.async.stubs.StubScheduler
import com.treode.cluster.{HostId, PortId}
import com.treode.pickle.Pickler

class StubNetwork private (implicit random: Random) {

  private val peers = new ConcurrentHashMap [HostId, StubCluster]

  var messageFlakiness = 0.0
  var messageTrace = false

  private [stubs] def install (peer: StubCluster): Unit =
    require (
        peers.putIfAbsent (peer.localId, peer) == null,
        s"Host ${peer.localId} is already installed.")

  private [stubs] def deliver [M] (p: Pickler [M], from: HostId, to: HostId, port: PortId, msg: M) {
    if (messageFlakiness > 0.0 && random.nextDouble < messageFlakiness)
      return
    val h = peers.get (to)
    require (h != null, s"Host $to is not installed.")
    if (messageTrace)
      println (s"$from->$to:$port: $msg")
    h.deliver (p, from, port, msg)
  }}

object StubNetwork {

  def apply (random: Random): StubNetwork =
    new StubNetwork () (random)
}
