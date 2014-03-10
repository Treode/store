package com.treode.store.catalog

import com.treode.async.{Callback, Fiber}
import com.treode.cluster.{MessageDescriptor, Peer}
import com.treode.store.CatalogId
import com.treode.store.paxos.BallotNumber

import Callback.ignore

private class Acceptor (val key: CatalogId, kit: CatalogKit) {
  import kit.{broker, cluster, scheduler}
  import kit.acceptors.remove
  import kit.config.{closedLifetime, deliberatingTimeout}

  private val fiber = new Fiber (scheduler)
  var state: State = null

  trait State {

    def query (proposer: Peer, ballot: Long)
    def propose (proposer: Peer, ballot: Long, patch: Patch)
    def choose (chosen: Update)

    def shutdown(): Unit =
      state = new Shutdown
  }

  class Opening extends State {

    def query (proposer: Peer, ballot: Long): Unit =
      state = new Getting (_.query (proposer, ballot))

    def propose (proposer: Peer, ballot: Long, patch: Patch): Unit =
      state = new Getting (_.propose (proposer, ballot, patch))

    def choose (chosen: Update): Unit =
      state = new Getting (_.choose (chosen))
  }

  class Getting (var op: State => Unit) extends State {

    broker.get (key) run (new Callback [Handler] {

      def pass (cat: Handler): Unit = fiber.execute {
        if (state == Getting.this) {
          state = new Deliberating (cat)
          op (state)
        }}

      def fail (t: Throwable): Unit = fiber.execute {
        if (state == Getting.this) {
          state = new Panicked (t)
          throw t
        }}})

    def query (proposer: Peer, ballot: Long): Unit =
      op = (_.query (proposer, ballot))

    def propose (proposer: Peer, ballot: Long, patch: Patch): Unit =
      op = (_.propose (proposer, ballot, patch))

    def choose (chosen: Update): Unit =
      op = (_.choose (chosen))
  }

  class Deliberating (handler: Handler) extends State {

    var ballot: BallotNumber = BallotNumber.zero
    var proposal: Proposal = Option.empty
    var proposers = Set.empty [Peer]

    fiber.delay (deliberatingTimeout) {
      if (state == Deliberating.this) {
        val default = Patch (handler.version, handler.checksum, Seq.empty)
        kit.propose (key, default)
            .map (Acceptor.this.choose (_))
            .run (ignore)
      }}

    def query (proposer: Peer, _ballot: Long) {
      proposers += proposer
      val ballot = BallotNumber (_ballot, proposer.id)
      if (ballot < this.ballot) {
        Proposer.refuse (key, this.ballot.number) (proposer)
      } else {
        this.ballot = ballot
        Proposer.promise (key, ballot.number, proposal) (proposer)
      }}

    def propose (proposer: Peer, _ballot: Long, patch: Patch) {
      proposers += proposer
      val ballot = BallotNumber (_ballot, proposer.id)
      if (ballot < this.ballot) {
        Proposer.refuse (key, this.ballot.number) (proposer)
      } else {
        this.ballot = ballot
        this.proposal = Some ((ballot, patch))
        Proposer.accept (key, ballot.number) (proposer)
      }}

    def choose (chosen: Update) {
      state = new Closed (chosen)
    }

    override def toString = s"Acceptor.Deliberating($key, $proposal)"
  }

  class Closed (val chosen: Update) extends State {

    broker.patch (key, chosen) run (new Callback [Unit] {

      def pass (v: Unit): Unit =
        fiber.delay (closedLifetime) (remove (key, Acceptor.this))

      def fail (t: Throwable): Unit = fiber.execute {
        if (state == Closed.this)
          state = new Panicked (t)
      }})

    def query (proposer: Peer, ballot: Long): Unit =
      Proposer.chosen (key, chosen) (proposer)

    def propose (proposer: Peer, ballot: Long, patch: Patch): Unit =
      Proposer.chosen (key, chosen) (proposer)

    def choose (chosen: Update): Unit =
      require (chosen.checksum == this.chosen.checksum, "Paxos disagreement")

    override def toString = s"Acceptor.Closed($key, $chosen)"
  }

  class Shutdown extends State {

    fiber.delay (closedLifetime) (remove (key, Acceptor.this))

    def query (proposer: Peer, ballot: Long): Unit = ()
    def propose (proposer: Peer, ballot: Long, patch: Patch): Unit = ()
    def choose (chosen: Update): Unit = ()

    override def shutdown() = ()

    override def toString = s"Acceptor.Shutdown($key)"
  }

  class Panicked (thrown: Throwable) extends Shutdown {

    override def toString = s"Acceptor.Panicked($key, $thrown)"
  }

  def query (proposer: Peer, ballot: Long): Unit =
    fiber.execute (state.query (proposer, ballot))

  def propose (proposer: Peer, ballot: Long, patch: Patch): Unit =
    fiber.execute (state.propose (proposer, ballot, patch))

  def choose (chosen: Update): Unit =
    fiber.execute (state.choose (chosen))

  def shutdown(): Unit =
    fiber.execute (state.shutdown())

  override def toString = state.toString
}

private object Acceptor {

  val query = {
    import CatalogPicklers._
    MessageDescriptor (0xFF9BFCEDF7D2E129L, tuple (catId, ulong))
  }

  val propose = {
    import CatalogPicklers._
    MessageDescriptor (0xFF3E59E358D49679L, tuple (catId, ulong, patch))
  }

  val choose = {
    import CatalogPicklers._
    MessageDescriptor (0xFF3CF1687A498C79L, tuple (catId, patch))
  }}
