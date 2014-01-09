package com.treode.store.cluster.paxos

import scala.language.postfixOps

import com.treode.async.{Fiber, callback}
import com.treode.cluster.{MessageDescriptor, Peer}
import com.treode.cluster.misc.{BackoffTimer, RichInt}
import com.treode.store.{Bytes, StorePicklers}

private class Acceptor (key: Bytes, kit: PaxosKit) {
  import kit.host.scheduler

  val deliberatingTimeout = 2 seconds
  val closedLifetime = 2 seconds

  private val fiber = new Fiber (scheduler)
  var state: State = new Restoring

  trait State {
    def query (from: Peer, ballot: Long, default: Bytes)
    def propose (from: Peer, ballot: Long, value: Bytes)
    def choose (value: Bytes)
    def timeout()
    def shutdown()
  }

  class Restoring extends State {

    def restore (default: Bytes): Unit =
      state = new Deliberating (default, BallotNumber.zero, None)

    def query (from: Peer, ballot: Long, default: Bytes) {
      restore (default)
      state.query (from, ballot, default)
    }

    def propose (from: Peer, ballot: Long, value: Bytes) {
      restore (value)
      state.propose (from, ballot, value)
    }

    def choose (value: Bytes) {
      restore (value)
      state.choose (value)
    }

    def timeout(): Unit =
      throw new IllegalStateException

    def shutdown(): Unit =
      state = new Shutdown

    override def toString = "Acceptor.Restoring (%s)" format (key)
  }

  class Deliberating (
      val default: Bytes,
      var ballot: BallotNumber,
      var proposal: Proposal) extends State {

    fiber.delay (deliberatingTimeout) (state.timeout())

    var proposers = Set [Peer] ()

    def query (from: Peer, _ballot: Long, default: Bytes) {
      proposers += from
      val ballot = BallotNumber (_ballot, from.id)
      if (ballot >= this.ballot) {
        Proposer.promise (key, _ballot, proposal) (from)
        this.ballot = ballot
      } else {
        Proposer.refuse (key, this.ballot.number) (from)
      }}

    def propose (from: Peer, _ballot: Long, value: Bytes) {
      proposers += from
      val ballot = BallotNumber (_ballot, from.id)
      if (ballot >= this.ballot) {
        Proposer.accept (key, _ballot) (from)
        this.ballot = ballot
        this.proposal = Some ((ballot, value))
      }}

    def choose (value: Bytes): Unit =
      state = new Closed (value)

    def timeout(): Unit =
      kit.propose (key, default, callback (Acceptor.this.choose (_)))

    def shutdown(): Unit =
      state = new Shutdown

    override def toString = "Acceptor.Deliberating " + (key, proposal)
  }

  class Closed (val chosen: Bytes) extends State {

    // TODO: Purge acceptor from memory once it is saved.
    //fiber.delay (closedLifetime) (remove (key, Acceptor.this))

    def query (from: Peer, ballot: Long, default: Bytes): Unit =
      Proposer.chosen (key, chosen) (from)

    def propose (from: Peer, ballot: Long, value: Bytes): Unit =
      Proposer.chosen (key, chosen) (from)

    def choose (value: Bytes): Unit =
      require (value == chosen, "Paxos disagreement")

    def timeout(): Unit = ()

    def shutdown(): Unit =
      state = new Shutdown

    override def toString = "Acceptor.Closed " + (key, chosen)
  }

  class Shutdown extends State {

    def query (from: Peer, ballot: Long, abort: Bytes): Unit = ()
    def propose (from: Peer, ballot: Long, value: Bytes): Unit = ()
    def choose (v: Bytes): Unit = ()
    def timeout(): Unit = ()
    def shutdown(): Unit = ()

    override def toString = "Acceptor.Shutdown (%s)" format (key)
  }

  def query (from: Peer, ballot: Long, default: Bytes): Unit =
    fiber.execute (state.query (from, ballot, default))

  def propose (from: Peer, ballot: Long, value: Bytes): Unit =
    fiber.execute (state.propose (from, ballot, value))

  def choose (value: Bytes): Unit =
    fiber.execute (state.choose (value))

  def shutdown(): Unit =
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
