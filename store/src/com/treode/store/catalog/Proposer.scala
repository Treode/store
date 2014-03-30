package com.treode.store.catalog

import java.util.concurrent.TimeoutException
import scala.language.postfixOps

import com.treode.async.{AsyncImplicits, Backoff, Fiber}
import com.treode.async.misc.RichInt
import com.treode.cluster.{MessageDescriptor, Peer}
import com.treode.store.CatalogId
import com.treode.store.paxos.BallotNumber

import AsyncImplicits._

private class Proposer (key: CatalogId, kit: CatalogKit) {
  import kit.proposers.remove
  import kit.{cluster, locate, random, scheduler}

  private val proposingBackoff = Backoff (200, 300, 1 minutes, 7)
  private val confirmingBackoff = Backoff (200, 300, 1 minutes, 7)
  private val closedLifetime = 2 seconds

  private val fiber = new Fiber (scheduler)
  var state: State = Opening

  trait State {
    def open (ballot: Long, patch: Patch) = ()
    def learn (k: Learner)
    def refuse (ballot: Long)
    def promise (from: Peer, ballot: Long, proposal: Proposal)
    def accept (from: Peer, ballot: Long)
    def chosen (value: Update)
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

  private def agreement (x: Proposal, patch: Patch) = {
    x match {
      case Some ((_, patch)) => patch
      case None => patch
    }}

  private def illegal = throw new IllegalStateException

  object Opening extends State {

    override def open (ballot: Long, patch: Patch) =
      state = new Open (ballot, patch)

    def learn (k: Learner) = throw new IllegalStateException

    def refuse (ballot: Long) = ()

    def promise (from: Peer, ballot: Long, proposal: Proposal) = ()

    def accept (from: Peer, ballot: Long) = ()

    def chosen (v: Update): Unit =
      state = new Closed (v)

    def timeout() = ()

    override def toString = "Proposer.Open (%s)" format (key.toString)
  }

  class Open (_ballot: Long, patch: Patch) extends State {

    var learners = List.empty [Learner]
    var ballot = _ballot
    var refused = ballot
    var proposed = Option.empty [(BallotNumber, Patch)]
    val promised = locate()
    val accepted = locate()

    // Ballot number zero was implicitly accepted.
    if (ballot == 0)
      Acceptor.propose (key, ballot, patch) (promised)
    else
      Acceptor.query (key, ballot) (promised)

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
          val v = agreement (proposed, patch)
          Acceptor.propose (key, ballot, v) (accepted)
        }}}

    def accept (from: Peer, ballot: Long) {
      if (ballot == this.ballot) {
        accepted += from
        if (accepted.quorum) {
          val v = agreement (proposed, patch)
          Acceptor.choose (key, v) (locate())
          learners foreach (_.pass (v))
          state = new Closed (v)
        }}}

    def chosen (v: Update) {
      learners foreach (_.pass (v))
      state = new Closed (v)
    }

    def timeout() {
      if (backoff.hasNext) {
        promised.clear()
        accepted.clear()
        ballot = refused + random.nextInt (17) + 1
        refused = ballot
        Acceptor.query (key, ballot) (promised)
        fiber.delay (backoff.next) (state.timeout())
      } else {
        remove (key, Proposer.this)
        learners foreach (_.fail (new TimeoutException))
      }}

    override def toString = "Proposer.Open " + (key, ballot, patch)
  }

  class Closed (update: Update) extends State {

    fiber.delay (closedLifetime) (remove (key, Proposer.this))

    def learn (k: Learner) =
      k.pass (update)

    def chosen (v: Update) =
      require (v == update, "Paxos disagreement")

    def refuse (ballot: Long) = ()
    def promise (from: Peer, ballot: Long, proposal: Proposal) = ()
    def accept (from: Peer, ballot: Long) = ()
    def timeout() = ()

    override def toString = "Proposer.Closed " + (key, update)
  }

  object Shutdown extends State {

    def learn (k: Learner) = ()
    def refuse (ballot: Long) = ()
    def promise (from: Peer, ballot: Long, proposal: Proposal) = ()
    def accept (from: Peer, ballot: Long) = ()
    def chosen (v: Update) = ()
    def timeout() = ()

    override def toString = "Proposer.Shutdown (%s)" format (key)
  }

  def open (ballot: Long, patch: Patch) =
    fiber.execute (state.open (ballot, patch))

  def learn (k: Learner) =
    fiber.execute  (state.learn (k))

  def refuse (ballot: Long) =
    fiber.execute  (state.refuse (ballot))

  def promise (from: Peer, ballot: Long, proposal: Proposal) =
    fiber.execute  (state.promise (from, ballot, proposal))

  def accept (from: Peer, ballot: Long) =
    fiber.execute  (state.accept (from, ballot))

  def chosen (updatre: Update) =
    fiber.execute  (state.chosen (updatre))

  def shutdown() =
    fiber.execute  (state.shutdown())

  override def toString = state.toString
}

private object Proposer {

  val refuse = {
    import CatalogPicklers._
    MessageDescriptor (0xFF8562E9071168EAL, tuple (catId, ulong))
  }

  val promise = {
    import CatalogPicklers._
    MessageDescriptor (0xFF3F6FFC9993CD75L, tuple (catId, ulong, proposal))
  }

  val accept = {
    import CatalogPicklers._
    MessageDescriptor (0xFF0E7973CC65E95FL, tuple (catId, ulong))
  }

  val chosen = {
    import CatalogPicklers._
    MessageDescriptor (0xFF2259321F9D4EF9L, tuple (catId, update))
  }}
