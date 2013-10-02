package com.treode.cluster

import java.net.InetSocketAddress
import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.Random

import com.treode.cluster.fiber.Scheduler
import com.treode.cluster.events.Events
import com.treode.cluster.messenger.{MailboxRegistry, Listener, PeerRegistry}
import com.treode.cluster.misc.{RichBoolean, RichOption, parseInetSocketAddress, parseInt}
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

    _mailboxes = new MailboxRegistry () (Events.live)

    _group = AsynchronousChannelGroup.withFixedThreadPool (1, Executors.defaultThreadFactory)

    _peers = PeerRegistry.live (localId, _group, _mailboxes) (_random, _scheduler, Events.live)
    for ((a, i) <- addresses.zipWithIndex)
      _peers.get (i) .address = a

    _listener = new Listener (localId, addresses (localId.id.toInt), _group, _peers) (_scheduler, Events.live)
    _listener.startup()

    implicit val host = new Host {
      val localId = EchoTest.this.localId
      val mailboxes = _mailboxes
      val peers = _peers
    }

    Echo.attach()

  } catch {
    case e: Throwable =>
      _fork (_shutdown())
      throw e
  }}

object EchoTest {

  private val usage = "usage: EchoTest id p0 p1"

  def main (args: Array[String] ) {
    (args.length == 3) orDie usage
    val id = parseInt (args (0)) getOrDie usage
    val p0 = parseInetSocketAddress (args (1)) getOrDie usage
    val p1 = parseInetSocketAddress (args (2)) getOrDie usage
    new EchoTest (id, Seq (p0, p1))
  }}
