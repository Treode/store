package com.treode.cluster.stubs

import java.util.concurrent.{ConcurrentHashMap, Executors}
import scala.collection.JavaConversions._
import scala.util.Random

import com.treode.async.stubs.StubScheduler
import com.treode.cluster.{HostId, PortId}
import com.treode.pickle.Pickler

import StubNetwork.inactive

class StubNetwork private (implicit random: Random) {

  private val peers = new ConcurrentHashMap [HostId, StubPeer]

  var messageFlakiness = 0.0
  var messageTrace = false

  private [stubs] def install (peer: StubPeer) {
    if (peers.putIfAbsent (peer.localId, peer) != null &&
        !peers.replace (peer.localId, inactive, peer))
      throw new IllegalArgumentException (s"Host ${peer.localId} is already installed.")
  }

  private [stubs] def remove (peer: StubPeer) {
    if (!peers.replace (peer.localId, peer, inactive) &&
        peers.get (peer.localId) != inactive)
      throw new IllegalArgumentException (s"Host ${peer.localId} was rebooted.")
  }

  private [stubs] def deliver [M] (p: Pickler [M], from: HostId, to: HostId, port: PortId, msg: M) {
    if (messageFlakiness > 0.0 && random.nextDouble < messageFlakiness)
      return
    val h = peers.get (to)
    require (h != null, s"Host $to does not exist.")
    if (messageTrace)
      println (s"$from->$to:$port: $msg $h")
    h.deliver (p, from, port, msg)
  }

  def active (id: HostId): Boolean = {
    val p = peers.get (id)
    p != null && p != inactive
  }}

object StubNetwork {

  private [stubs] val inactive =
    new StubPeer (0) (null, null, null) {
      override def deliver [M] (p: Pickler [M], from: HostId, port: PortId, msg: M) = ()
    }

  def apply (random: Random): StubNetwork =
    new StubNetwork () (random)
}
