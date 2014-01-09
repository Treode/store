package com.treode.store.cluster.paxos

import java.util.concurrent.TimeoutException
import scala.language.postfixOps

import com.treode.async.{Callback, Fiber}
import com.treode.cluster.{Peer, MessageDescriptor}
import com.treode.cluster.misc.{BackoffTimer, RichInt}
import com.treode.store.Bytes

private class Proposer (key: Bytes, kit: PaxosKit) {
  import kit.Acceptors.locate
  import kit.Proposers.remove
  import kit.host
  import kit.host.{random, scheduler}

  private val proposingBackoff = BackoffTimer (200, 300, 1 minutes, 7) (random)
  private val confirmingBackoff = BackoffTimer (200, 300, 1 minutes, 7) (random)
  private val closedLifetime = 2 seconds

  private val fiber = new Fiber (scheduler)
  var state: State = Opening

  trait State {
    def open (ballot: Long, value: Bytes) = ()
    def learn (k: Learner)
    def refuse (ballot: Long)
    def promise (from: Peer, ballot: Long, proposal: Proposal)
    def accept (from: Peer, ballot: Long)
    def chosen (value: Bytes)
    def timeout()
    def shutdown() = state = Shutdown
  }

  private def max (x: Proposal, y: Proposal) = {
    if (x.isDefined && y.isDefined) {
      if (x.get._1 > y.get._1) x else y
    } else if (x.isDefined) {
      x
    } else if (y.isDefined) {
      y
    } else {
      None
    }}

  private def agreement (x: Proposal, value: Bytes) = {
    x match {
      case Some ((_, value)) => value
      case None => value
    }}

  private def illegal = throw new IllegalStateException

  object Opening extends State {

    override def open (ballot: Long, value: Bytes) =
      state = new Open (ballot, value)

    def learn (k: Learner) = throw new IllegalStateException

    def refuse (ballot: Long) = ()

    def promise (from: Peer, ballot: Long, proposal: Proposal) = ()

    def accept (from: Peer, ballot: Long) = ()

    def chosen (v: Bytes): Unit =
      state = new Closed (v)

    def timeout() = ()

    override def toString = "Proposer.Open (%s)" format (key.toString)
  }

  class Open (_ballot: Long, value: Bytes) extends State {

    var learners = List.empty [Learner]
    var ballot = _ballot
    var refused = ballot
    var proposed = Option.empty [(BallotNumber, Bytes)]
    val promised = locate (key)
    val accepted = locate (key)

    // Ballot number zero was implicitly accepted.
    if (ballot == 0)
      Acceptor.propose (key, ballot, value) (promised)
    else
      Acceptor.query (key, ballot, value) (promised)

    val backoff = proposingBackoff.iterator
    fiber.delay (backoff.next) (state.timeout())

    def learn (k: Learner) =
      learners ::= k

    def refuse (ballot: Long) = {
      refused = math.max (refused, ballot)
      promised.clear()
      accepted.clear()
    }

    def promise (from: Peer, ballot: Long, proposal: Proposal) {
      if (ballot == this.ballot) {
        promised += from
        proposed = max (proposed, proposal)
        if (promised.quorum) {
          val v = agreement (proposed, value)
          Acceptor.propose (key, ballot, v) (accepted)
        }}}

    def accept (from: Peer, ballot: Long) {
      if (ballot == this.ballot) {
        accepted += from
        if (accepted.quorum) {
          val v = agreement (proposed, value)
          Acceptor.choose (key, v) (locate (key))
          learners foreach (_ (v))
          state = new Closed (v)
        }}}

    def chosen (v: Bytes) {
      learners foreach (_ (v))
      state = new Closed (v)
    }

    def timeout() {
      if (backoff.hasNext) {
        promised.clear()
        accepted.clear()
        ballot = refused + random.nextInt (17) + 1
        refused = ballot
        Acceptor.query (key, ballot, value) (promised)
        fiber.delay (backoff.next) (state.timeout())
      } else {
        remove (key, Proposer.this)
        learners foreach (_.fail (new TimeoutException))
      }}

    override def toString = "Proposer.Open " + (key, ballot, value)
  }

  class Closed (value: Bytes) extends State {

    fiber.delay (closedLifetime) (remove (key, Proposer.this))

    def learn (k: Learner) =
      k (value)

    def chosen (v: Bytes) =
      require (v == value, "Paxos disagreement")

    def refuse (ballot: Long) = ()
    def promise (from: Peer, ballot: Long, proposal: Proposal) = ()
    def accept (from: Peer, ballot: Long) = ()
    def timeout() = ()

    override def toString = "Proposer.Closed " + (key, value)
  }

  object Shutdown extends State {

    def learn (k: Learner) = ()
    def refuse (ballot: Long) = ()
    def promise (from: Peer, ballot: Long, proposal: Proposal) = ()
    def accept (from: Peer, ballot: Long) = ()
    def chosen (v: Bytes) = ()
    def timeout() = ()

    override def toString = "Proposer.Shutdown (%s)" format (key)
  }

  def open (ballot: Long, value: Bytes) =
    fiber.execute (state.open (ballot, value))

  def learn (k: Learner) =
    fiber.execute  (state.learn (k))

  def refuse (ballot: Long) =
    fiber.execute  (state.refuse (ballot))

  def promise (from: Peer, ballot: Long, proposal: Proposal) =
    fiber.execute  (state.promise (from, ballot, proposal))

  def accept (from: Peer, ballot: Long) =
    fiber.execute  (state.accept (from, ballot))

  def chosen (value: Bytes) =
    fiber.execute  (state.chosen (value))

  def shutdown() =
    fiber.execute  (state.shutdown())

  override def toString = state.toString
}

private object Proposer {

  val refuse = {
    import PaxosPicklers._
    new MessageDescriptor (0xFF3725D9448D98D0L, tuple (bytes, long))
  }

  val promise = {
    import PaxosPicklers._
    new MessageDescriptor (0xFF52232E0CCEE1D2L, tuple (bytes, long, proposal))
  }

  val accept = {
    import PaxosPicklers._
    new MessageDescriptor (0xFFB799D0E495804BL, tuple (bytes, long))
  }

  val chosen = {
    import PaxosPicklers._
    new MessageDescriptor (0xFF3D8DDECF0F6CBEL, tuple (bytes, bytes))
  }}
