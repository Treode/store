package com.treode.store.cluster.paxos

import scala.language.postfixOps

import com.treode.async.{Callback, Fiber, callback}
import com.treode.cluster.{MessageDescriptor, Peer}
import com.treode.cluster.misc.{BackoffTimer, RichInt}
import com.treode.store.{Bytes, StorePicklers}
import com.treode.store.disk2.LogEntry

import LogEntry.{PaxosAccept, PaxosClose, PaxosOpen, PaxosPromise, PaxosReaccept}

private class Acceptor (key: Bytes, kit: PaxosKit) {
  import Acceptor.{Post, noop}
  import kit.disks
  import kit.host.scheduler

  val deliberatingTimeout = 2 seconds
  val closedLifetime = 2 seconds

  private val fiber = new Fiber (scheduler)
  var state: State = new Restoring

  val recorded = new Callback [Unit] {
    def pass (v: Unit): Unit = fiber.execute (state.recorded())
    def fail (t: Throwable): Unit = fiber.execute (state = new Panicked (t))
  }

  def open (ballot: Long, default: Bytes, from: Peer): Post =
    new Post {
      def record = disks.record (PaxosOpen (key, default), recorded)
      def reply() = Proposer.promise (key, ballot, None) (from)
    }

  def promise (ballot: BallotNumber, proposal: Proposal, from: Peer): Post =
    new Post {
      def record = disks.record (PaxosPromise (key, ballot), recorded)
      def reply() = Proposer.promise (key, ballot.number, proposal) (from)
    }

  def accept (ballot: BallotNumber, value: Bytes, from: Peer): Post =
    new Post {
      def record() = disks.record (PaxosAccept (key, ballot, value), recorded)
      def reply() = Proposer.accept (key, ballot.number) (from)
    }

  def reaccept (ballot: BallotNumber, from: Peer): Post =
    new Post {
      def record() = disks.record (PaxosReaccept (key, ballot), recorded)
      def reply() = Proposer.accept (key, ballot.number) (from)
    }

  trait State {
    def query (from: Peer, ballot: Long, default: Bytes)
    def propose (from: Peer, ballot: Long, value: Bytes)
    def choose (value: Bytes)
    def recorded()
    def timeout (default: Bytes)
    def shutdown()
  }

  class Restoring extends State {

    def restore (from: Peer, ballot: Long, default: Bytes): Unit =
      state = Deliberating.record (from, ballot, default)

    def query (from: Peer, ballot: Long, default: Bytes) {
      restore (from, ballot, default)
      state.query (from, ballot, default)
    }

    def propose (from: Peer, ballot: Long, value: Bytes) {
      restore (from, ballot, value)
      state.propose (from, ballot, value)
    }

    def choose (value: Bytes) {
      state = Closed.record (value)
    }

    def recorded(): Unit =
      throw new IllegalStateException

    def timeout (default: Bytes): Unit =
      throw new IllegalStateException

    def shutdown(): Unit =
      state = new Shutdown

    override def toString = s"Acceptor.Restoring($key)"
  }

  class Deliberating private (var posting: Post) extends State {

    var proposers = Set.empty [Peer]
    var ballot = BallotNumber.zero
    var proposal: Proposal = Option.empty

    var postable = noop

    def post (post: Post) {
      if (posting == noop) {
        posting = post
        posting.record()
      } else {
        this.postable = post
      }}

    def query (from: Peer, _ballot: Long, default: Bytes) {
      proposers += from
      val ballot = BallotNumber (_ballot, from.id)
      if (ballot < this.ballot) {
        Proposer.refuse (key, this.ballot.number) (from)
      } else {
        post (promise (ballot, proposal, from))
        this.ballot = ballot
      }}

    def propose (from: Peer, _ballot: Long, value: Bytes) {
      proposers += from
      val ballot = BallotNumber (_ballot, from.id)
      if (ballot < this.ballot) {
        Proposer.refuse (key, this.ballot.number) (from)
      } else {
        if (proposal.isDefined && value == proposal.get._2)
          post (reaccept (ballot, from))
        else
          post (accept (ballot, value, from))
        this.ballot = ballot
        this.proposal = Some ((ballot, value))
      }}

    def choose (value: Bytes): Unit =
      state = Closed.record (value)

    def recorded() {
      posting.reply()
      posting = postable
      postable = noop
      posting.record()
    }

    def timeout (default: Bytes): Unit =
      kit.propose (key, default, callback (Acceptor.this.choose (_)))

    def shutdown(): Unit =
      state = new Shutdown

    override def toString = s"Acceptor.Deliberating($key, $proposal)"
  }

  object Deliberating {

    def record (from: Peer, ballot: Long, default: Bytes): State = {
      fiber.delay (deliberatingTimeout) (state.timeout (default))
      val post = open (ballot, default, from)
      post.record()
      new Deliberating (post)
    }}

  class Closed private (val chosen: Bytes) extends State {

    // TODO: Purge acceptor from memory once it is saved.
    //fiber.delay (closedLifetime) (remove (key, Acceptor.this))

    def query (from: Peer, ballot: Long, default: Bytes): Unit =
      Proposer.chosen (key, chosen) (from)

    def propose (from: Peer, ballot: Long, value: Bytes): Unit =
      Proposer.chosen (key, chosen) (from)

    def choose (value: Bytes): Unit =
      require (value == chosen, "Paxos disagreement")

    def recorded(): Unit = ()

    def timeout (default: Bytes): Unit = ()

    def shutdown(): Unit =
      state = new Shutdown

    override def toString = s"Acceptor.Closed($key, $chosen)"
  }

  object Closed {

    def record (chosen: Bytes): State = {
      disks.record (PaxosClose (key, chosen), Callback.ignore)
      new Closed (chosen)
    }}

  class Shutdown extends State {

    def query (from: Peer, ballot: Long, abort: Bytes): Unit = ()
    def propose (from: Peer, ballot: Long, value: Bytes): Unit = ()
    def choose (v: Bytes): Unit = ()
    def recorded(): Unit = ()
    def timeout (default: Bytes): Unit = ()
    def shutdown(): Unit = ()

    override def toString = s"Acceptor.Shutdown ($key)"
  }

  class Panicked (t: Throwable) extends State {

    def query (from: Peer, ballot: Long, abort: Bytes): Unit = ()
    def propose (from: Peer, ballot: Long, value: Bytes): Unit = ()
    def choose (v: Bytes): Unit = ()
    def recorded(): Unit = ()
    def timeout (default: Bytes): Unit = ()
    def shutdown(): Unit = ()

    override def toString = s"Acceptor.Panicked ($t)"
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

  trait Post {
    def record()
    def reply()
  }

  val noop: Post =
    new Post {
      def record() = ()
      def reply() = ()
    }

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
