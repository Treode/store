package com.treode.cluster

import java.net.SocketAddress
import scala.language.postfixOps
import scala.util.Random

import com.treode.cluster.events.EventsStub
import com.treode.cluster.messenger.{MailboxRegistry, PeerRegistry}
import com.treode.concurrent.{Scheduler, StubScheduler}
import com.treode.pickle.{Buffer, Pickler, pickle}

abstract class ClusterStubBase (seed: Long, nhosts: Int) {

  private val emptyAddr = new SocketAddress {}

  class HostStubBase (val localId: HostId) extends Host {

    val random: Random = ClusterStubBase.this.random

    val scheduler: Scheduler = ClusterStubBase.this.scheduler

    val mailboxes: MailboxRegistry = new MailboxRegistry () (EventsStub)

    val peers: PeerRegistry =
      new PeerRegistry (localId, new ConnectionStub (ClusterStubBase.this, _, localId)) (random)

    def locate (id: Int): Acknowledgements =
      ClusterStubBase.this.locate (id)

    def cleanup(): Unit = ()

    private [cluster] def deliver (id: MailboxId, from: HostId, msg: Buffer): Unit =
      mailboxes.deliver (id, peers.get (from), msg, msg.readableBytes)
  }

  type HostStub <: HostStubBase

  def newHost (id: HostId): HostStub

  val random = new Random (seed)

  val scheduler = StubScheduler (random)

  var messageTrace = false
  var messageFlakiness = 0.0

  val hosts = Seq.fill (nhosts) (newHost (HostId (random.nextLong)))
  for (h1 <- hosts)
    for (h2 <- hosts)
      h1.peers.get (h2.localId) .address = emptyAddr

  private val hostById = hosts .map (h => (h.localId, h)) .toMap

  def locate (id: Int): Acknowledgements =
    Acknowledgements.settled (hosts map (_.localId): _*)

  def deliver [M] (p: Pickler [M], from: HostId, to: HostId, mbx: MailboxId, msg: M) {
    if (messageFlakiness == 0.0 || random.nextDouble > messageFlakiness) {
      val h = hostById.get (to)
      require (h.isDefined, s"$to does not exist.")
      if (messageTrace)
        println (s"$from->$to:$mbx: $msg")
      val buf = Buffer (12)
      pickle (p, msg, buf)
      h.get.deliver (mbx, from, buf)
    }}

  def runTasks(): Unit = scheduler.runTasks()

  def cleanup(): Unit =
    hosts.foreach (_.cleanup())
}
