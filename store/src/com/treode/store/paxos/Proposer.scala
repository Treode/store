/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode.store.paxos

import scala.language.postfixOps

import com.treode.async.{Backoff, Callback, Fiber}
import com.treode.async.implicits._
import com.treode.async.misc.RichInt
import com.treode.cluster.{MessageDescriptor, Peer, ReplyTracker}
import com.treode.store.{Atlas, BallotNumber, Bytes, TimeoutException, TxClock}

private class Proposer (key: Bytes, time: TxClock, kit: PaxosKit) {
  import kit.{cluster, library, random, scheduler}
  import kit.config.{closedLifetime, proposingBackoff}
  import kit.proposers.remove

  val fiber = new Fiber
  val backoff = proposingBackoff.iterator
  var learners = List.empty [Learner]
  var state: State = Opening

  trait State {
    def open (ballot: Long, value: Bytes) = ()
    def learn (k: Learner)
    def refuse (from: Peer, ballot: Long)
    def grant (from: Peer, ballot: Long, proposal: Proposal)
    def accept (from: Peer, ballot: Long)
    def chosen (value: Bytes)
    def timeout()
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

  private def closeWithSuccess (v: Bytes) {
    learners foreach (_.pass (v))
    learners = List.empty
    state = new ClosedWithSuccess (v)
  }

  private def closeWithTimeout() {
    remove (key, time, Proposer.this)
    learners foreach (scheduler.fail (_, new TimeoutException))
    learners = List.empty
    state = new ClosedWithTimeout
  }

  object Opening extends State {

    override def open (ballot: Long, value: Bytes) {
      // Ballot number zero was implicitly granted.
      if (ballot == 0)
        state = new Proposing (ballot, library.atlas, value)
      else
        state = new Asking (ballot, value)
      fiber.delay (backoff.next) (state.timeout())
    }

    def learn (k: Learner) = throw new IllegalStateException

    def refuse (from: Peer, ballot: Long) = ()

    def grant (from: Peer, ballot: Long, proposal: Proposal) = ()

    def accept (from: Peer, ballot: Long) = ()

    def chosen (v: Bytes): Unit =
      closeWithSuccess (v)

    def timeout() = throw new IllegalStateException

    override def toString = "Proposer.Opening (%s)" format (key.toString)
  }

  class Asking (var ballot: Long, value: Bytes) extends State {

    var refused = ballot
    var proposed = Option.empty [(BallotNumber, Bytes)]
    var atlas = library.atlas
    var granted = track (atlas, key, time)

    Acceptor.ask (atlas.version, key, time, ballot, value) (granted)

    def learn (k: Learner) =
      learners ::= k

    def refuse (from: Peer, ballot: Long) = {
      refused = math.max (refused, ballot)
      granted = track (atlas, key, time)
    }

    def grant (from: Peer, ballot: Long, proposal: Proposal) {
      if (ballot == this.ballot) {
        granted += from
        proposed = max (proposed, proposal)
        if (granted.quorum)
          state = new Proposing (ballot, atlas, agreement (proposed, value))
      }}

    def accept (from: Peer, ballot: Long): Unit = ()

    def chosen (v: Bytes): Unit =
      closeWithSuccess (v)

    def timeout() {
      if (backoff.hasNext) {
        atlas = library.atlas
        granted = track (atlas, key, time)
        ballot = refused + random.nextInt (17) + 1
        refused = ballot
        Acceptor.ask (atlas.version, key, time, ballot, value) (granted)
        fiber.delay (backoff.next) (state.timeout())
      } else {
        closeWithTimeout()
      }}

    override def toString = "Proposer.Asking " + (key, ballot, value)
  }

  class Proposing (ballot: Long, atlas: Atlas, value: Bytes) extends State {

    var refused = ballot
    var proposed = Option.empty [(BallotNumber, Bytes)]
    var accepted = track (atlas, key, time)

    Acceptor.propose (atlas.version, key, time, ballot, value) (accepted)

    val backoff = proposingBackoff.iterator

    def learn (k: Learner) =
      learners ::= k

    def refuse (from: Peer, ballot: Long) = {
      refused = math.max (refused, ballot)
      accepted = track (atlas, key, time)
    }

    def grant (from: Peer, ballot: Long, proposal: Proposal): Unit = ()

    def accept (from: Peer, ballot: Long) {
      if (ballot == this.ballot) {
        accepted += from
        if (accepted.quorum) {
          val v = agreement (proposed, value)
          Acceptor.choose (key, time, v) (track (atlas, key, time))
          closeWithSuccess (v)
        }}}

    def chosen (v: Bytes): Unit =
      closeWithSuccess (v)

    def timeout() {
      if (backoff.hasNext) {
        state = new Asking (refused + random.nextInt (17) + 1, value)
        fiber.delay (backoff.next) (state.timeout())
      } else {
        closeWithTimeout()
      }}

    override def toString = "Proposer.Proposing " + (key, ballot, value)
  }

  class ClosedWithSuccess (value: Bytes) extends State {

    fiber.delay (closedLifetime) (remove (key, time, Proposer.this))

    def learn (k: Learner) =
      scheduler.pass (k, value)

    def chosen (v: Bytes) =
      assert (v == value, "Paxos disagreement")

    def refuse (from: Peer, ballot: Long) = ()

    def grant (from: Peer, ballot: Long, proposal: Proposal) = ()

    def accept (from: Peer, ballot: Long) = ()

    def timeout() = ()

    override def toString = "Proposer.ClosedWithSuccess " + (key, value)
  }

  class ClosedWithTimeout extends State {

    fiber.delay (closedLifetime) (remove (key, time, Proposer.this))

    def learn (k: Learner) =
      scheduler.fail (k, new TimeoutException)

    def chosen (v: Bytes) =
      assert (false, "Paxos disagreement")

    def refuse (from: Peer, ballot: Long) = ()

    def grant (from: Peer, ballot: Long, proposal: Proposal) = ()

    def accept (from: Peer, ballot: Long) = ()

    def timeout() = ()

    override def toString = s"Proposer.ClosedWithTimeout ($key)"
  }

  object Shutdown extends State {

    def learn (k: Learner) = ()
    def refuse (from: Peer, ballot: Long) = ()
    def grant (from: Peer, ballot: Long, proposal: Proposal) = ()
    def accept (from: Peer, ballot: Long) = ()
    def chosen (v: Bytes) = ()
    def timeout() = ()

    override def toString = "Proposer.Shutdown (%s)" format (key)
  }

  def open (ballot: Long, value: Bytes) =
    fiber.execute {
      //println (s"open $key $ballot $value $state")
      state.open (ballot, value)
    }

  def learn (k: Learner) =
    fiber.execute  {
      //println (s"learn $key $state")
      state.learn (k)
    }

  def refuse (from: Peer, ballot: Long) =
    fiber.execute  {
      //println (s"refuse $key $ballot $state")
      state.refuse (from, ballot)
    }

  def grant (from: Peer, ballot: Long, proposal: Proposal) =
    fiber.execute  {
      //println (s"grant $key $ballot $proposal $state")
      state.grant (from, ballot, proposal)
    }

  def accept (from: Peer, ballot: Long) =
    fiber.execute {
      //println (s"accept $key $ballot $state")
      state.accept (from, ballot)
    }

  def chosen (value: Bytes) =
    fiber.execute {
      //println (s"chosen $key $value $state")
      state.chosen (value)
    }

  override def toString = state.toString
}

private object Proposer {

  val refuse = {
    import PaxosPicklers._
    MessageDescriptor (0xFF3725D9448D98D0L, tuple (bytes, txClock, ulong))
  }

  val grant = {
    import PaxosPicklers._
    MessageDescriptor (0xFF52232E0CCEE1D2L, tuple (bytes, txClock, ulong, proposal))
  }

  val accept = {
    import PaxosPicklers._
    MessageDescriptor (0xFFB799D0E495804BL, tuple (bytes, txClock, ulong))
  }

  val chosen = {
    import PaxosPicklers._
    MessageDescriptor (0xFF3D8DDECF0F6CBEL, tuple (bytes, txClock, bytes))
  }}
