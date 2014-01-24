package com.treode.store.paxos

import scala.language.postfixOps
import com.treode.async.{Callback, Fiber, callback}
import com.treode.cluster.{MessageDescriptor, Peer}
import com.treode.cluster.misc.{BackoffTimer, RichInt}
import com.treode.store.{Bytes, StorePicklers}
import com.treode.disk.{PageDescriptor, RecordDescriptor, RootDescriptor}

private class Acceptor (key: Bytes, kit: PaxosKit) {
  import Acceptor.{NoPost, Post, Status}
  import kit.{disks, scheduler}

  val deliberatingTimeout = 2 seconds
  val closedLifetime = 2 seconds

  private val fiber = new Fiber (scheduler)
  var state: State = new Restoring

  trait State {
    def query (from: Peer, ballot: Long, default: Bytes)
    def propose (from: Peer, ballot: Long, value: Bytes)
    def choose (value: Bytes)
    def checkpoint (cb: Callback [Status])
    def recover (status: Status)
    def opened (default: Bytes)
    def promised (ballot: BallotNumber)
    def accepted (ballot: BallotNumber, value: Bytes)
    def reaccepted (ballot: BallotNumber)
    def closed (chosen: Bytes)
    def shutdown()
  }

  class Restoring extends State {

    def query (from: Peer, ballot: Long, default: Bytes) {
      state = Deliberating.record (from, ballot, default)
      state.query (from, ballot, default)
    }

    def propose (from: Peer, ballot: Long, value: Bytes) {
      state = Deliberating.record (from, ballot, value)
      state.propose (from, ballot, value)
    }

    def choose (value: Bytes): Unit =
      state = Closed.record (value)

    def checkpoint (cb: Callback [Status]): Unit =
      throw new IllegalStateException

    def recover (status: Status) {
      status match {
        case Status.Deliberating (_, default, ballot, proposal) =>
          state = Deliberating.recover (default, ballot, proposal)
        case Status.Closed (_, chosen) =>
          state = Closed.recover (chosen)
      }
      throw new IllegalStateException
    }

    def opened (default: Bytes): Unit =
      state = Deliberating.opened (default)

    def promised (ballot: BallotNumber): Unit =
      throw new IllegalStateException

    def accepted (ballot: BallotNumber, value: Bytes): Unit =
      throw new IllegalStateException

    def reaccepted (ballot: BallotNumber): Unit =
      throw new IllegalStateException

    def closed (chosen: Bytes): Unit =
      throw new IllegalStateException

    def shutdown(): Unit =
      throw new IllegalStateException

    override def toString = s"Acceptor.Restoring($key)"
  }

  class Deliberating private (
      val default: Bytes,
      var proposers: Set [Peer],
      var ballot: BallotNumber,
      var proposal: Proposal) extends State {

    var posting: Post = NoPost
    var postable: Post = NoPost

    val posted = new Callback [Unit] {

      def pass (v: Unit): Unit = fiber.execute {
        if (state == Deliberating.this) {
          posting.reply()
          posting = postable
          postable = NoPost
          posting.record()
        }}

      def fail (t: Throwable): Unit = fiber.execute {
        if (state == Deliberating.this) {
          val s = Status.Deliberating (key, default, ballot, proposal)
          state = new Panicked (s, t)
        }}}

    def post (post: Post) {
      if (posting == NoPost) {
        posting = post
        posting.record()
      } else {
        this.postable = post
      }}

    fiber.delay (deliberatingTimeout) {
      if (state == Deliberating.this)
        kit.propose (key, default, callback (Acceptor.this.choose (_)))
    }

    def open (ballot: Long, default: Bytes, from: Peer): Unit =
      post (new Post {
        def record = Acceptor.open (key, default) (posted)
        def reply() = Proposer.promise (key, ballot, None) (from)
      })

    def promise (ballot: BallotNumber, proposal: Proposal, from: Peer): Unit =
      post (new Post {
        def record = Acceptor.promise (key, ballot) (posted)
        def reply() = Proposer.promise (key, ballot.number, proposal) (from)
      })

    def accept (ballot: BallotNumber, value: Bytes, from: Peer): Unit =
      post (new Post {
        def record() = Acceptor.accept (key, ballot, value) (posted)
        def reply() = Proposer.accept (key, ballot.number) (from)
      })

    def reaccept (ballot: BallotNumber, from: Peer): Unit =
      post (new Post {
        def record() = Acceptor.reaccept (key, ballot) (posted)
        def reply() = Proposer.accept (key, ballot.number) (from)
      })

    def query (from: Peer, _ballot: Long, default: Bytes) {
      proposers += from
      val ballot = BallotNumber (_ballot, from.id)
      if (ballot < this.ballot) {
        Proposer.refuse (key, this.ballot.number) (from)
      } else {
        promise (ballot, proposal, from)
        this.ballot = ballot
      }}

    def propose (from: Peer, _ballot: Long, value: Bytes) {
      proposers += from
      val ballot = BallotNumber (_ballot, from.id)
      if (ballot < this.ballot) {
        Proposer.refuse (key, this.ballot.number) (from)
      } else {
        if (proposal.isDefined && value == proposal.get._2)
          reaccept (ballot, from)
        else
          accept (ballot, value, from)
        this.ballot = ballot
        this.proposal = Some ((ballot, value))
      }}

    def choose (value: Bytes): Unit =
      state = Closed.record (value)

    def checkpoint (cb: Callback [Status]): Unit =
      cb (Status.Deliberating (key, default, ballot, proposal))

    def recover (status: Status): Unit =
      throw new IllegalStateException

    def opened (default: Bytes): Unit = ()

    def promised (ballot: BallotNumber) {
      if (this.ballot < ballot)
        this.ballot = ballot
    }

    def accepted (ballot: BallotNumber, value: Bytes) {
      if (this.ballot < ballot) {
        this.ballot = ballot
        this.proposal = Some ((ballot, value))
      }}

    def reaccepted (ballot: BallotNumber) {
      val value = proposal.get._2
      if (this.ballot < ballot) {
        this.ballot = ballot
        this.proposal = Some ((ballot, value))
      }}

    def closed (chosen: Bytes) {
      state = Closed.record (chosen)
    }

    def shutdown(): Unit =
      state = new Shutdown (Status.Deliberating (key, default, ballot, proposal))

    override def toString = s"Acceptor.Deliberating($key, $proposal)"
  }

  object Deliberating {

    def record (from: Peer, ballot: Long, default: Bytes): State = {
      val s = new Deliberating (default, Set (from), BallotNumber.zero, Option.empty)
      s.open (ballot, default, from)
      s
    }

    def recover (default: Bytes, ballot: BallotNumber, proposal: Proposal): State =
      new Deliberating (default, Set.empty, ballot, proposal)

    def opened (default: Bytes): State =
      new Deliberating (default, Set.empty, BallotNumber.zero, Option.empty)
  }

  class Closed private (val chosen: Bytes) extends State {

    // TODO: Purge acceptor from memory once it is saved.
    //fiber.delay (closedLifetime) (remove (key, Acceptor.this))

    def query (from: Peer, ballot: Long, default: Bytes): Unit =
      Proposer.chosen (key, chosen) (from)

    def propose (from: Peer, ballot: Long, value: Bytes): Unit =
      Proposer.chosen (key, chosen) (from)

    def choose (value: Bytes): Unit =
      require (value == chosen, "Paxos disagreement")

    def checkpoint (cb: Callback [Status]): Unit =
      cb (Status.Closed (key, chosen))

    def recover (status: Status): Unit =
      throw new IllegalStateException

    def closed (chosen: Bytes): Unit =
      require (this.chosen == chosen, "Confused recovery.")

    def shutdown(): Unit =
      state = new Shutdown (Status.Closed (key, chosen))

    def opened (default: Bytes): Unit = ()
    def promised (ballot: BallotNumber): Unit = ()
    def accepted (ballot: BallotNumber, value: Bytes): Unit = ()
    def reaccepted (ballot: BallotNumber): Unit = ()

    override def toString = s"Acceptor.Closed($key, $chosen)"
  }

  object Closed {

    def record (chosen: Bytes): State = {
      Acceptor.close (key, chosen) (Callback.ignore)
      new Closed (chosen)
    }

    def recover (chosen: Bytes): State = {
      new Closed (chosen)
    }}

  class Shutdown (status: Status) extends State {

    def checkpoint (cb: Callback [Status]): Unit =
      cb (status)

    def recover (status: Status): Unit =
      throw new IllegalStateException

    def query (from: Peer, ballot: Long, abort: Bytes): Unit = ()
    def propose (from: Peer, ballot: Long, value: Bytes): Unit = ()
    def choose (v: Bytes): Unit = ()
    def opened (default: Bytes): Unit = ()
    def promised (ballot: BallotNumber): Unit = ()
    def shutdown(): Unit = ()
    def accepted (ballot: BallotNumber, value: Bytes): Unit = ()
    def reaccepted (ballot: BallotNumber): Unit = ()
    def closed (chosen: Bytes): Unit = ()

    override def toString = s"Acceptor.Shutdown ($key)"
  }

  class Panicked (status: Status, thrown: Throwable) extends Shutdown (status) {

    override def toString = s"Acceptor.Panicked ($key, $thrown)"
  }

  def query (from: Peer, ballot: Long, default: Bytes): Unit =
    fiber.execute (state.query (from, ballot, default))

  def propose (from: Peer, ballot: Long, value: Bytes): Unit =
    fiber.execute (state.propose (from, ballot, value))

  def choose (value: Bytes): Unit =
    fiber.execute (state.choose (value))

  def checkpoint (cb: Callback [Status]) =
    fiber.execute (state.checkpoint (cb))

  def recover (status: Status) =
    fiber.execute (state.recover (status))

  def opened (default: Bytes): Unit =
    fiber.execute (state.opened (default))

  def promised (ballot: BallotNumber): Unit =
    fiber.execute (state.promised (ballot))

  def accepted (ballot: BallotNumber, value: Bytes): Unit =
    fiber.execute (state.accepted (ballot, value))

  def reaccepted (ballot: BallotNumber): Unit =
    fiber.execute (state.reaccepted (ballot))

  def closed (chosen: Bytes): Unit =
    fiber.execute (state.closed (chosen))

  def shutdown(): Unit =
    fiber.execute (state.shutdown())

  override def toString = state.toString
}

private object Acceptor {

  sealed abstract class Status {
    def key: Bytes
  }

  object Status {

    case class Deliberating (
        key: Bytes, default:
        Bytes, ballot:
        BallotNumber,
        proposal: Proposal) extends Status

    case class Closed (key: Bytes, chosen: Bytes) extends Status

    val pickle = {
      import PaxosPicklers._
      tagged [Status] (
          0x1 -> wrap (bytes, bytes, ballotNumber, proposal)
                .build ((Deliberating.apply _).tupled)
                .inspect (v => (v.key, v.default, v.ballot, v.proposal)),
          0x2 -> wrap (bytes, bytes)
                .build ((Closed.apply _).tupled)
                .inspect (v => (v.key, v.chosen)))
    }}

  trait Post {
    def record()
    def reply()
  }

  object NoPost extends Post {
    def record() = ()
    def reply() = ()
  }

  val query = {
    import PaxosPicklers._
    new MessageDescriptor (0xFF14D4F00908FB59L, tuple (bytes, long, bytes))
  }

  val propose = {
    import PaxosPicklers._
    new MessageDescriptor (0xFF09AFD4F9B688D9L, tuple (bytes, long, bytes))
  }

  val choose = {
    import PaxosPicklers._
    new MessageDescriptor (0xFF761FFCDF5DEC8BL, tuple (bytes, bytes))
  }

  val root = {
    import PaxosPicklers._
    new RootDescriptor (0xBFD4F3D3, position)
  }

  val openTable = {
    import PaxosPicklers._
    new PageDescriptor (0x7C71E2AF, const (0), seq (acceptorStatus))
  }

  val open = {
    import PaxosPicklers._
    new RecordDescriptor (0x77784AB1, tuple (bytes, bytes))
  }

  val promise = {
    import PaxosPicklers._
    new RecordDescriptor (0x32A1544B, tuple (bytes, ballotNumber))
  }

  val accept = {
    import PaxosPicklers._
    new RecordDescriptor (0xD6CCC0BE, tuple (bytes, ballotNumber, bytes))
  }

  val reaccept = {
    import PaxosPicklers._
    new RecordDescriptor (0x52720640, tuple (bytes, ballotNumber))
  }

  val close = {
    import PaxosPicklers._
    new RecordDescriptor (0xAE980885, tuple (bytes, bytes))
  }}
