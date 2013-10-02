package com.treode.cluster.messenger

import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions._
import scala.util.Random

import com.treode.cluster.{HostId, Peer}
import com.treode.cluster.events.Events
import com.treode.cluster.fiber.{Fiber, Scheduler}

class PeerRegistry (localId: HostId, newPeer: HostId => Peer) (implicit random: Random) {

  private val peers = new ConcurrentHashMap [HostId, Peer]

  def get (id: HostId): Peer = {
    val p0 = peers.get (id)
    if (p0 == null) {
      val p1 = newPeer (id)
      val p2 = peers.putIfAbsent (id, p1)
      if (p2 == null) p1 else p2
    } else {
      p0
    }}

  def rpeer: Option [Peer] = {
    if (peers.size > 2) {
      val n = random.nextInt (peers.size - 1)
      val i = peers.valuesIterator.filter (_.id != localId) .drop (n)
      if (i.hasNext) Some (i.next) else None
    } else if (peers.size == 2) {
      val i = peers.valuesIterator.filter (_.id != localId)
      if (i.hasNext) Some (i.next) else None
    } else {
      None
    }}

  def shutdown(): Unit =
    peers.values foreach (_.close())

  override def toString =
    "PeerRegistry" +
        (peers map (kv => (kv._1, kv._2)) mkString ("(\n    ", ",\n    ", ")"))
}

object PeerRegistry {

  def live (localId: HostId, group: AsynchronousChannelGroup, mailboxes: MailboxRegistry) (
      implicit random: Random, scheduler: Scheduler, events: Events): PeerRegistry = {

    def newPeer (remoteId: HostId): Peer =
      if (remoteId == localId)
        new LocalConnection (localId, mailboxes)
      else
        new RemoteConnection (remoteId, localId, new Fiber, group, mailboxes)

    new PeerRegistry (localId, newPeer)
  }}
