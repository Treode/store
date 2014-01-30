package com.treode.store.paxos

import scala.language.postfixOps

import com.treode.async.{Callback, Fiber, callback}
import com.treode.cluster.{MessageDescriptor, Peer}
import com.treode.cluster.misc.{BackoffTimer, RichInt}
import com.treode.disk.{PageDescriptor, RecordDescriptor, RootDescriptor}
import com.treode.store.{Bytes, StorePicklers}
import com.treode.store.simple.SimpleTable

private class Acceptor (val key: Bytes, kit: PaxosKit) {
  import Acceptor.{NoPost, Post, Status}
  import kit.{cluster, disks, scheduler}
  import kit.acceptors.db
  import kit.config.{closedLifetime, deliberatingTimeout}

  private val fiber = new Fiber (scheduler)
  var state: State = null

  trait State {
    def query (proposer: Peer, ballot: Long, default: Bytes)
    def propose (proposer: Peer, ballot: Long, value: Bytes)
    def choose (chosen: Bytes)
    def checkpoint (cb: Callback [Status])
    def shutdown()
  }

  class Opening extends State {

    def query (proposer: Peer, ballot: Long, default: Bytes) {
      val s = new Restoring (default)
      s.restore (_.query (proposer, ballot, default))
      state = s
    }

    def propose (proposer: Peer, ballot: Long, value: Bytes) {
      val s = new Restoring (value)
      s.restore (_.propose (proposer, ballot, value))
      state = s
    }

    def choose (chosen: Bytes) {
      val s = new Restoring (chosen)
      s.restore (_.choose (chosen))
      state = s
    }

    def checkpoint (cb: Callback [Status]): Unit =
      throw new IllegalStateException

    def shutdown(): Unit =
      throw new IllegalStateException
  }

  class Restoring (default: Bytes) extends State {

    var ballot: BallotNumber = BallotNumber.zero
    var proposal: Proposal = Option.empty
    var proposers = Set.empty [Peer]
    var postable = {_: Deliberating => ()}

    def promise (ballot: BallotNumber, proposal: Proposal, proposer: Peer): Unit =
      postable = (_.promise (ballot, proposal, proposer))

    def accept (ballot: BallotNumber, value: Bytes, proposer: Peer): Unit =
      postable = (_.accept (ballot, value, proposer))

    def restore (f: State => Unit) {
      db.get (key, new Callback [Option [Bytes]] {

        def pass (chosen: Option [Bytes]): Unit = fiber.execute {
          if (state == Restoring.this) {
            state = chosen match {
              case Some (value) =>
                Proposer.chosen (key, value) (proposers)
                new Closed (value)
              case None =>
                val s = new Deliberating (default, ballot, proposal, proposers)
                postable (s)
                s
            }
            f (state)
          }}

        def fail (t: Throwable): Unit = fiber.execute {
          if (state == Restoring.this) {
            state = new Panicked (Status.Restoring (key, default), t)
            throw t
          }}})
    }

    def query (proposer: Peer, _ballot: Long, default: Bytes) {
      proposers += proposer
      val ballot = BallotNumber (_ballot, proposer.id)
      if (this.ballot <= ballot) {
        promise (ballot, proposal, proposer)
        this.ballot = ballot
      }}

    def propose (proposer: Peer, _ballot: Long, value: Bytes) {
      proposers += proposer
      val ballot = BallotNumber (_ballot, proposer.id)
      if (this.ballot <= ballot) {
        accept (ballot, value, proposer)
        this.ballot = ballot
        this.proposal = Some ((ballot, value))
      }}

    def choose (chosen: Bytes) {
      state = new Closed (chosen)
      val gen  = db.put (key, chosen)
      Acceptor.close.record (key, chosen, gen) (Callback.ignore)
      Proposer.chosen (key, chosen) (proposers)
    }

    def checkpoint (cb: Callback [Status]): Unit =
      cb (Status.Restoring (key, default))

    def shutdown(): Unit =
      state = new Shutdown (Status.Restoring (key, default))

    override def toString = s"Acceptor.Restoring($key)"
  }

  class Deliberating (
      val default: Bytes,
      var ballot: BallotNumber,
      var proposal: Proposal,
      var proposers: Set [Peer]) extends State {

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

    def open (ballot: Long, default: Bytes, proposer: Peer): Unit =
      post (new Post {
        def record = Acceptor.open.record (key, default) (posted)
        def reply() = Proposer.promise (key, ballot, None) (proposer)
      })

    def promise (ballot: BallotNumber, proposal: Proposal, proposer: Peer): Unit =
      post (new Post {
        def record = Acceptor.promise.record (key, ballot) (posted)
        def reply() = Proposer.promise (key, ballot.number, proposal) (proposer)
      })

    def accept (ballot: BallotNumber, value: Bytes, proposer: Peer): Unit =
      post (new Post {
        def record() = Acceptor.accept.record (key, ballot, value) (posted)
        def reply() = Proposer.accept (key, ballot.number) (proposer)
      })

    def reaccept (ballot: BallotNumber, proposer: Peer): Unit =
      post (new Post {
        def record() = Acceptor.reaccept.record (key, ballot) (posted)
        def reply() = Proposer.accept (key, ballot.number) (proposer)
      })

    def query (proposer: Peer, _ballot: Long, default: Bytes) {
      proposers += proposer
      val ballot = BallotNumber (_ballot, proposer.id)
      if (ballot < this.ballot) {
        Proposer.refuse (key, this.ballot.number) (proposer)
      } else {
        promise (ballot, proposal, proposer)
        this.ballot = ballot
      }}

    def propose (proposer: Peer, _ballot: Long, value: Bytes) {
      proposers += proposer
      val ballot = BallotNumber (_ballot, proposer.id)
      if (ballot < this.ballot) {
        Proposer.refuse (key, this.ballot.number) (proposer)
      } else {
        if (proposal.isDefined && value == proposal.get._2)
          reaccept (ballot, proposer)
        else
          accept (ballot, value, proposer)
        this.ballot = ballot
        this.proposal = Some ((ballot, value))
      }}

    def choose (chosen: Bytes) {
      state = new Closed (chosen)
      val gen  = db.put (key, chosen)
      Acceptor.close.record (key, chosen, gen) (Callback.ignore)
    }

    def checkpoint (cb: Callback [Status]): Unit =
      cb (Status.Deliberating (key, default, ballot, proposal))

    def shutdown(): Unit =
      state = new Shutdown (Status.Deliberating (key, default, ballot, proposal))

    override def toString = s"Acceptor.Deliberating($key, $proposal)"
  }

  class Closed (val chosen: Bytes) extends State {

    // TODO: Purge acceptor from memory once tests are fixed.
    // fiber.delay (closedLifetime) (remove (key, Acceptor.this))

    def query (proposer: Peer, ballot: Long, default: Bytes): Unit =
      Proposer.chosen (key, chosen) (proposer)

    def propose (proposer: Peer, ballot: Long, value: Bytes): Unit =
      Proposer.chosen (key, chosen) (proposer)

    def choose (chosen: Bytes): Unit =
      require (chosen == this.chosen, "Paxos disagreement")

    def checkpoint (cb: Callback [Status]): Unit =
      cb (Status.Closed (key, chosen))

    def shutdown(): Unit =
      state = new Shutdown (Status.Closed (key, chosen))

    override def toString = s"Acceptor.Closed($key, $chosen)"
  }

  class Shutdown (status: Status) extends State {

    def checkpoint (cb: Callback [Status]): Unit =
      cb (status)

    def query (proposer: Peer, ballot: Long, abort: Bytes): Unit = ()
    def propose (proposer: Peer, ballot: Long, value: Bytes): Unit = ()
    def choose (chosen: Bytes): Unit = ()
    def shutdown(): Unit = ()

    override def toString = s"Acceptor.Shutdown($key)"
  }

  class Panicked (status: Status, thrown: Throwable) extends Shutdown (status) {

    override def toString = s"Acceptor.Panicked($key, $thrown)"
  }

  def query (proposer: Peer, ballot: Long, default: Bytes): Unit =
    fiber.execute (state.query (proposer, ballot, default))

  def propose (proposer: Peer, ballot: Long, value: Bytes): Unit =
    fiber.execute (state.propose (proposer, ballot, value))

  def choose (chosen: Bytes): Unit =
    fiber.execute (state.choose (chosen))

  def checkpoint (cb: Callback [Status]) =
    fiber.execute (state.checkpoint (cb))

  def shutdown(): Unit =
    fiber.execute (state.shutdown())

  override def toString = state.toString
}

private object Acceptor {

  val ClosedTable = 0xDD683792

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

  val statii = {
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
    new RecordDescriptor (0xAE980885, tuple (bytes, bytes, long))
  }

  trait Post {
    def record()
    def reply()
  }

  object NoPost extends Post {
    def record() = ()
    def reply() = ()
  }

  sealed abstract class Status

  object Status {

    case class Restoring (key: Bytes, default: Bytes) extends Status

    case class Deliberating (
        key: Bytes,
        default: Bytes,
        ballot: BallotNumber,
        proposal: Proposal) extends Status

    case class Closed (key: Bytes, chosen: Bytes) extends Status {
      def default = chosen
    }

    val pickler = {
      import PaxosPicklers._
      tagged [Status] (
          0x1 -> wrap (bytes, bytes)
                 .build ((Restoring.apply _).tupled)
                 .inspect (v => (v.key, v.default)),
          0x2 -> wrap (bytes, bytes, ballotNumber, proposal)
                 .build ((Deliberating.apply _).tupled)
                 .inspect (v => (v.key, v.default, v.ballot, v.proposal)),
          0x3 -> wrap (bytes, bytes)
                 .build ((Closed.apply _).tupled)
                 .inspect (v => (v.key, v.chosen)))
    }}}
