package com.treode.store.catalog

import scala.util.{Failure, Success}

import com.treode.async.{Callback, Fiber}
import com.treode.cluster.{MessageDescriptor, Peer}
import com.treode.store.{BallotNumber, CatalogId}

import Callback.ignore

private class Acceptor (val key: CatalogId, val version: Int, kit: CatalogKit) {
  import kit.{acceptors, broker, cluster, scheduler}
  import kit.config.{closedLifetime, deliberatingTimeout}

  private val fiber = new Fiber
  var state: State = null

  trait State {
    def query (proposer: Peer, ballot: Long, default: Patch)
    def propose (proposer: Peer, ballot: Long, patch: Patch)
    def choose (chosen: Patch)
  }

  private def panic (s: State, t: Throwable): Unit =
    fiber.execute {
      if (state == s) {
        state = new Panicked (t)
        throw t
      }}

  class Opening extends State {

    def query (proposer: Peer, ballot: Long, default: Patch): Unit =
      state = new Getting (_.query (proposer, ballot, default), default)

    def propose (proposer: Peer, ballot: Long, patch: Patch): Unit =
      state = new Getting (_.propose (proposer, ballot, patch), patch)

    def choose (chosen: Patch): Unit =
      state = new Getting (_.choose (chosen), chosen)

    override def toString = "Acceptor.Opening"
  }

  class Getting (var op: State => Unit, default: Patch) extends State {

    broker.get (key) run {
      case Success (cat) => got (cat)
      case Failure (t) => panic (Getting.this, t)
    }

    def got (cat: Handler): Unit =
      fiber.execute {
        if (state == Getting.this) {
          state = new Deliberating (cat, default)
          op (state)
        }}

    def query (proposer: Peer, ballot: Long, default: Patch): Unit =
      op = (_.query (proposer, ballot, default))

    def propose (proposer: Peer, ballot: Long, patch: Patch): Unit =
      op = (_.propose (proposer, ballot, patch))

    def choose (chosen: Patch): Unit =
      op = (_.choose (chosen))

    override def toString = "Acceptor.Getting"
  }

  class Deliberating (handler: Handler, default: Patch) extends State {

    var ballot: BallotNumber = BallotNumber.zero
    var proposal: Proposal = Option.empty
    var proposers = Set.empty [Peer]

    fiber.delay (deliberatingTimeout) (timeout())

    def timeout() {
      if (state == Deliberating.this)
        kit.propose (key, default) .run {
          case Success (v) => Acceptor.this.choose (v)
          case Failure (_) => timeout()
        }}

    def query (proposer: Peer, _ballot: Long, default: Patch) {
      proposers += proposer
      val ballot = BallotNumber (_ballot, proposer.id)
      if (ballot < this.ballot) {
        Proposer.refuse (key, version, this.ballot.number) (proposer)
      } else {
        this.ballot = ballot
        Proposer.promise (key, version, ballot.number, proposal) (proposer)
      }}

    def propose (proposer: Peer, _ballot: Long, patch: Patch) {
      proposers += proposer
      val ballot = BallotNumber (_ballot, proposer.id)
      if (ballot < this.ballot) {
        Proposer.refuse (key, version, this.ballot.number) (proposer)
      } else {
        this.ballot = ballot
        this.proposal = Some ((ballot, patch))
        Proposer.accept (key, version, ballot.number) (proposer)
      }}

    def choose (chosen: Patch) {
      state = new Closed (chosen)
    }

    override def toString = s"Acceptor.Deliberating($key, $proposal)"
  }

  class Closed (val chosen: Patch) extends State {

    broker.patch (key, chosen) run {
      case Success (v) =>
        fiber.delay (closedLifetime) (acceptors.remove (key, version, Acceptor.this))
      case Failure (t) =>
        panic (Closed.this, t)
    }

    def query (proposer: Peer, ballot: Long, default: Patch): Unit =
      Proposer.chosen (key, version, chosen) (proposer)

    def propose (proposer: Peer, ballot: Long, patch: Patch): Unit =
      Proposer.chosen (key, version, chosen) (proposer)

    def choose (chosen: Patch): Unit =
      require (chosen.checksum == this.chosen.checksum, "Paxos disagreement")

    override def toString = s"Acceptor.Closed($key, $chosen)"
  }

  class Panicked (thrown: Throwable) extends State {

    fiber.delay (closedLifetime) (acceptors.remove (key, version, Acceptor.this))

    def query (proposer: Peer, ballot: Long, default: Patch): Unit = ()
    def propose (proposer: Peer, ballot: Long, patch: Patch): Unit = ()
    def choose (chosen: Patch): Unit = ()

    override def toString = s"Acceptor.Panicked($key, $thrown)"
  }

  def query (proposer: Peer, ballot: Long, default: Patch): Unit =
    fiber.execute (state.query (proposer, ballot, default))

  def propose (proposer: Peer, ballot: Long, patch: Patch): Unit =
    fiber.execute (state.propose (proposer, ballot, patch))

  def choose (chosen: Patch): Unit =
    fiber.execute (state.choose (chosen))

  override def toString = state.toString
}

private object Acceptor {

  val query = {
    import CatalogPicklers._
    MessageDescriptor (0xFF9BFCEDF7D2E129L, tuple (uint, catId, uint, ulong, patch))
  }

  val propose = {
    import CatalogPicklers._
    MessageDescriptor (0xFF3E59E358D49679L, tuple (uint, catId, uint, ulong, patch))
  }

  val choose = {
    import CatalogPicklers._
    MessageDescriptor (0xFF3CF1687A498C79L, tuple (catId, uint, patch))
  }}
