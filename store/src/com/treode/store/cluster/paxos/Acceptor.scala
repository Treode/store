package com.treode.store.cluster.paxos

import scala.language.postfixOps

import com.treode.async.{Callback, Fiber}
import com.treode.cluster.{MessageDescriptor, Peer}
import com.treode.cluster.misc.{BackoffTimer, RichInt}
import com.treode.store.{Bytes, StorePicklers}

private class Acceptor (key: Bytes, kit: PaxosKit) {
  import kit.Acceptors.{db, locate, remove}
  import kit.host.scheduler

  val deliberatingTimeout = 2 seconds
  val closedLifetime = 2 seconds

  private val fiber = new Fiber (scheduler)
  var state: State = Restoring

  trait State {
    def query (from: Peer, ballot: Long, default: Bytes, cb: Callback [Unit])
    def propose (from: Peer, ballot: Long, value: Bytes, cb: Callback [Unit])
    def choose (value: Bytes, cb: Callback [Unit])
    def timeout()
    def shutdown()
  }

  object Restoring extends State {

    def restore (default: Bytes, cb: Callback [Unit]) (f: State => Unit) {
      Callback.guard (cb) {
        db.get (key, Callback.unary {
          case Some (PaxosStatus.Deliberating (v, n, p)) =>
            state = new Deliberating (v, n, p)
            f (state)
          case Some (PaxosStatus.Closed (v)) =>
            state = new Closed (v)
            f (state)
          case None =>
            state = new Deliberating (default, BallotNumber.zero, None)
            f (state)
          })
      }}

    def query (from: Peer, ballot: Long, default: Bytes, cb: Callback [Unit]) =
      restore (default, cb) (_.query (from, ballot, default, cb))

    def propose (from: Peer, ballot: Long, value: Bytes, cb: Callback [Unit]) =
      restore (value, cb) (_.propose (from, ballot, value, cb))

    def choose (value: Bytes, cb: Callback [Unit]) =
      restore (value, cb) (_.choose (value, cb))

    def timeout() =
      throw new IllegalStateException

    def shutdown() =
      throw new IllegalStateException

    override def toString = "Acceptor.Restoring (%s)" format (key)
  }

  class Deliberating (
      val default: Bytes,
      var ballot: BallotNumber,
      var proposal: Proposal) extends State {

    fiber.delay (deliberatingTimeout) (state.timeout())

    val acceptors = locate (key)

    var proposers = Set [Peer] ()

    def query (from: Peer, _ballot: Long, default: Bytes, cb: Callback [Unit]): Unit =
      Callback.guard (cb) {
        proposers += from
        val ballot = BallotNumber (_ballot, from.id)
        if (ballot >= this.ballot) {
          Proposer.promise (key, _ballot, proposal) (from)
          this.ballot = ballot
        } else {
          Proposer.refuse (key, this.ballot.number) (from)
        }
        cb()
      }

    def propose (from: Peer, _ballot: Long, value: Bytes, cb: Callback [Unit]): Unit =
      Callback.guard (cb) {
        proposers += from
        val ballot = BallotNumber (_ballot, from.id)
        if (ballot >= this.ballot) {
          Proposer.accept (key, _ballot) (from)
          this.ballot = ballot
          this.proposal = Some ((ballot, value))
        }
        cb()
      }

    def choose (value: Bytes, cb: Callback [Unit]): Unit =
      Callback.guard (cb) {
        state = Closed (value)
        cb()
      }

    def timeout() {
      kit.propose (key, default, new Callback [Bytes] {
        def pass (v: Bytes) = Acceptor.this.choose (v)
        def fail (t: Throwable) = throw t
      })
    }

    def shutdown() {
      state = Shutdown
      db.put (key, PaxosStatus.Deliberating (default, ballot, proposal))
    }

    override def toString = "Acceptor.Deliberating " + (key, proposal)
  }

  class Closed (val chosen: Bytes) extends State {

    fiber.delay (closedLifetime) (remove (key, Acceptor.this))

    def query (from: Peer, ballot: Long, default: Bytes, cb: Callback [Unit]): Unit =
      Callback.guard (cb) {
        Proposer.chosen (key, chosen) (from)
        cb()
      }

    def propose (from: Peer, ballot: Long, value: Bytes, cb: Callback [Unit]): Unit =
      Callback.guard (cb) {
        Proposer.chosen (key, chosen) (from)
        cb()
      }

    def choose (value: Bytes, cb: Callback [Unit]): Unit =
      Callback.guard (cb) {
        require (value == chosen, "Paxos disagreement")
        cb()
      }

    def timeout() = ()

    def shutdown() =
      state = Shutdown

    override def toString = "Acceptor.Closed " + (key, chosen)
  }

  object Closed {

    def apply (v: Bytes): Closed = {
      db.put (key, PaxosStatus.Closed (v))
      new Closed (v)
    }}

  object Shutdown extends State {

    def query (from: Peer, ballot: Long, abort: Bytes, cb: Callback [Unit]) = cb()
    def propose (from: Peer, ballot: Long, value: Bytes, cb: Callback [Unit]) = cb()
    def choose (v: Bytes, cb: Callback [Unit]) = cb()
    def timeout() = ()
    def shutdown() = ()

    override def toString = "Acceptor.Shutdown (%s)" format (key)
  }

  def query (from: Peer, ballot: Long, default: Bytes) =
    fiber.begin (state.query (from, ballot, default, _))

  def propose (from: Peer, ballot: Long, value: Bytes) =
    fiber.begin (state.propose (from, ballot, value, _))

  def choose (value: Bytes) =
    fiber.begin (state.choose (value, _))

  def shutdown() =
    fiber.execute (state.shutdown())

  override def toString = state.toString
}

private object Acceptor {

  val query = {
    import StorePicklers._
    new MessageDescriptor (0xFF14D4F00908FB59L, tuple (bytes, long, bytes))
  }

  val propose = {
    import StorePicklers._
    new MessageDescriptor (0xFF09AFD4F9B688D9L, tuple (bytes, long, bytes))
  }

  val choose = {
    import StorePicklers._
    new MessageDescriptor (0xFF761FFCDF5DEC8BL, tuple (bytes, bytes))
  }}
