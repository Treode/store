package com.treode.cluster

import java.net.{SocketAddress, InetSocketAddress}
import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.Random

import com.treode.async.Scheduler
import com.treode.async.misc._
import com.treode.pickle.Pickler
import sun.misc.{SignalHandler, Signal}

class EchoTest (localId: HostId, addresses: Seq [InetSocketAddress]) {

  private val _random = Random
  private val _exiting = new AtomicBoolean (false)

  private var _executor: ScheduledExecutorService = null
  private var _scheduler: Scheduler = null
  private var _mailboxes: MailboxRegistry = null
  private var _group: AsynchronousChannelGroup = null
  private var _peers: PeerRegistry = null
  private var _listener: Listener = null

  private def _fork (f: => Unit) {
    new Thread() {
      override def run(): Unit = f
    }.start()
  }

  private def _shutdown (name: String, cond: Boolean) (f: => Any) {
    if (cond) {
      println ("Shutting down " + name)
      try {
        f
      } catch {
        case e: Throwable =>
          println ("Error shutting down " + name)
          e.printStackTrace()
      }}}

  def _shutdown() {

    if (!_exiting.compareAndSet (false, true))
      return

    _shutdown ("messenger listener", _listener != null) (_listener.shutdown())

    _shutdown ("messenger connections", _peers != null) {
      _peers.shutdown()
    }

    _shutdown ("messenger group", _group != null) {
      _group.shutdown()
      _group.awaitTermination (2, TimeUnit.SECONDS)
    }
    _shutdown ("messenger group by force", _group != null && !_group.isTerminated) {
      _group.shutdownNow()
    }

    _shutdown ("executor", _executor != null) {
      _executor.shutdown()
      _executor.awaitTermination (2, TimeUnit.SECONDS)
    }
    _shutdown ("executor by force", _executor != null && !_executor.isTerminated) {
      _executor.shutdownNow()
    }

    println ("Bye-bye.")
    System.exit (0)
  }

  Signal.handle (new Signal ("INT"), new SignalHandler {
    def handle (s: Signal): Unit = _shutdown()
  })

  Signal.handle (new Signal ("TERM"), new SignalHandler {
    def handle (s: Signal): Unit = _shutdown()
  })

  try {

    val nt = Runtime.getRuntime.availableProcessors
    println ("Using " + nt + " threads")
    _executor = Executors.newScheduledThreadPool (nt)
    _scheduler = Scheduler (_executor)

    _mailboxes = new MailboxRegistry

    _group = AsynchronousChannelGroup.withFixedThreadPool (1, Executors.defaultThreadFactory)

    _peers = PeerRegistry.live (localId, _group, _mailboxes) (_random, _scheduler)
    for ((a, i) <- addresses.zipWithIndex)
      _peers.get (i) .address = a

    _listener = new Listener (localId, addresses (localId.id.toInt), _group, _peers) (_scheduler)
    _listener.startup()

    val cluster = new Cluster {

      private val scuttlebutt: Scuttlebutt =
        new Scuttlebutt (localId, _peers) (_scheduler)

      def listen [M] (desc: MessageDescriptor [M]) (f: (M, Peer) => Any): Unit =
        _mailboxes.listen (desc.pmsg, desc.id) (f)

      def listen [M] (desc: RumorDescriptor [M]) (f: (M, Peer) => Any): Unit =
        scuttlebutt.listen (desc) (f)

      def peer (id: HostId): Peer =
        _peers.get (id)

      def rpeer: Option [Peer] =
        _peers.rpeer

      def hail (remoteId: HostId, remoteAddr: SocketAddress): Unit =
        _peers.get (remoteId) .address = remoteAddr

      def open [M] (p: Pickler [M], s: Scheduler): EphemeralMailbox [M] =
        _mailboxes.open (p, s)

      def spread [M] (desc: RumorDescriptor [M]) (msg: M): Unit =
      scuttlebutt.spread (desc) (msg)
    }

    Echo.attach (localId) (_random, _scheduler, cluster)

  } catch {
    case e: Throwable =>
      _fork (_shutdown())
      throw e
  }}

object EchoTest {

  private val usage = "usage: EchoTest p0 p1 p2 id"

  def main (args: Array[String] ) {
    (args.length == 4) orDie usage
    new EchoTest (
      parseInt (args (3)) getOrDie usage,
      Seq (
        parseInetSocketAddress (args (0)) getOrDie usage,
        parseInetSocketAddress (args (1)) getOrDie usage,
        parseInetSocketAddress (args (2)) getOrDie usage))
  }}
