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

import scala.util.Random

import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.cluster.Peer
import com.treode.cluster.stubs.{MessageCaptor, StubNetwork}
import com.treode.store.{BallotNumber, Bytes}
import org.scalatest.FreeSpec

import PaxosTestTools._

class ProposerSpec extends FreeSpec {

  private implicit class RichPeer (p: Peer) {

    def refuse (key: Long, ballot: Long): Unit =
      Proposer.refuse ((Bytes (key), 0, ballot)) (p)

    def grant (key: Long, ballot: Long): Unit =
      Proposer.grant ((Bytes (key), 0, ballot, None)) (p)

    def grant (key: Long, proposedBallot: Long, acceptedBallot: Int, acceptedValue: Int) {
      val proposal = (BallotNumber (acceptedBallot, 0), Bytes (acceptedValue))
      Proposer.grant ((Bytes (key), 0, proposedBallot, Some (proposal))) (p)
    }

    def accept (key: Long, ballot: Long): Unit =
      Proposer.accept ((Bytes (key), 0, ballot)) (p)

    def chosen (key: Long, value: Int): Unit =
      Proposer.chosen (key, 0, value) (p)
  }

  private trait Request {

    def key: Long
    def ballot: Long
    def from: Peer

    def refuse(): Unit =
      from.refuse (key, this.ballot)

    def grant(): Unit =
      from.grant (key, ballot)

    def grant (ballot: Int, value: Int): Unit =
      from.grant (key, this.ballot, ballot, value)

    def accept(): Unit =
      from.accept (key, ballot)

    def chosen (value: Int): Unit =
      from.chosen (key, value)
  }

  private case class Ask (key: Long, ballot: Long, default: Int, from: Peer) extends Request

  private case class Propose (key: Long, ballot: Long, value: Int, from: Peer) extends Request

  private case class Choose (key: Long, value: Int, from: Peer)

  private implicit class RichMessageCaptor (c: MessageCaptor) {

    def expectAsk() (implicit s: StubScheduler): Ask = {
      val ((_, key, _, ballot, default), from) = c.expect (Acceptor.ask)
      Ask (key.long, ballot, default.int, from)
    }

    def expectAsk (key: Long, ballot: Long, default: Int) (implicit s: StubScheduler) {
      val q = c.expectAsk()
      assertResult ((key, ballot, default)) ((q.key, q.ballot, q.default))
    }

    def expectPropose () (implicit s: StubScheduler): Propose = {
      val ((_, key, _, ballot, value), from) = c.expect (Acceptor.propose)
      Propose (key.long, ballot, value.int, from)
    }

    def expectPropose (key: Long, ballot: Long, value: Int) (implicit s: StubScheduler): Propose = {
      val q = expectPropose()
      assertResult ((key, ballot, value)) ((q.key, q.ballot, q.value))
      q
    }

    def expectChoose () (implicit s: StubScheduler): Choose = {
      val ((key, _, value), from) = c.expect (Acceptor.choose)
      Choose (key.long, value.int, from)
    }

    def expectChoose (key: Long, value: Int) (implicit s: StubScheduler) {
      val q = expectChoose()
      assertResult ((key, value)) ((q.key, q.value))
    }}

  val k1 = 0x135511175FBAA7D7L
  val k2 = 0x14CBC07329299C07L
  val v1 = 0x9BE837E1
  val v2 = 0x6003A752

  private def setupKit() = {
    val r = new Random (0)
    val s = StubScheduler.random (r)
    val n = StubNetwork (r)
    (r, s, n)
  }

  private def setupThreeAcceptors() (implicit r: Random, s: StubScheduler, n: StubNetwork) = {
    val p = StubPaxosHost.install() .expectPass()
    val a1 = MessageCaptor.install()
    val a2 = MessageCaptor.install()
    val a3 = MessageCaptor.install()
    p.setAtlas (settled (a1, a2, a3))
    (p, Seq (a1, a2, a3))
  }

  private def expectAsk (as: Seq [MessageCaptor], k: Long, v: Int) (implicit s: StubScheduler) = {
    val qs = as.map (_.expectAsk())
    val q = qs.head
    assertResult ((k, v)) ((q.key, q.default))
    qs.tail.foreach (assertResult (q) (_))
    qs
  }

  "The proposer should ask, and when" - {

    "refused, it should ask again" in {
      implicit val (random, scheduler, network) = setupKit()
      val (p, as) = setupThreeAcceptors()
      val c = p.propose (k1, v1) .capture()
      val Seq (q, _, _) = expectAsk (as, k1, v1)
      q.refuse()
      expectAsk (as, k1, v1)
      as foreach (_.expectNone())
      c.assertNotInvoked()
    }

    "granted and told no current proposal, it should propose its own value" in {
      implicit val (random, scheduler, network) = setupKit()
      val (p, as) = setupThreeAcceptors()
      val c = p.propose (k1, v1) .capture()
      val Seq (q1, q2, q3) = expectAsk (as, k1, v1)
      q1.grant()
      q2.grant()
      as foreach (_.expectPropose (k1, q1.ballot, v1))
      q3.grant()
      as foreach (_.expectNone())
      c.assertNotInvoked()
    }

    "granted and told a current proposal, it should propose that itself" in {
      implicit val (random, scheduler, network) = setupKit()
      val (p, as) = setupThreeAcceptors()
      val c = p.propose (k1, v1) .capture()
      val Seq (q1, q2, q3) = expectAsk (as, k1, v1)
      q1.grant (0, v2)
      q2.grant()
      as foreach (_.expectPropose (k1, q1.ballot, v2))
      q3.grant()
      as foreach (_.expectNone())
      c.assertNotInvoked()
    }

    "told a value chosen value, it should inform the learner" in {
      implicit val (random, scheduler, network) = setupKit()
      val (p, as) = setupThreeAcceptors()
      val c = p.propose (k1, v1) .capture()
      val Seq (q, _, _) = expectAsk (as, k1, v1)
      q.chosen (v2)
      c.expectPass (v2)
    }

    "ignored, it should ask again" in {
      implicit val (random, scheduler, network) = setupKit()
      val (p, as) = setupThreeAcceptors()
      val c = p.propose (k1, v1) .capture()
      expectAsk (as, k1, v1)
      expectAsk (as, k1, v1)
      as foreach (_.expectNone())
      assert (!c.wasInvoked)
    }

    "ignored twice, it should ask twice" in {
      implicit val (random, scheduler, network) = setupKit()
      val (p, as) = setupThreeAcceptors()
      val c = p.propose (k1, v1) .capture()
      expectAsk (as, k1, v1)
      expectAsk (as, k1, v1)
      expectAsk (as, k1, v1)
      assert (!c.wasInvoked)
    }}

  "The proposer should propose, and when" - {

    def setup (as: Seq [MessageCaptor], k: Long, v: Int) (implicit s: StubScheduler) = {
      val qs1 @ Seq (q1, q2, _) = expectAsk (as, k, v)
      q1.grant()
      q2.grant()
      val qs2 = as map (_.expectPropose (k, q1.ballot, v))
      (qs1, qs2)
    }

    "it receives an older refusal, it should ignore it" in {
      implicit val (random, scheduler, network) = setupKit()
      val (p, as) = setupThreeAcceptors()
      val c = p.propose (k1, v1) .capture()
      val (Seq (q, _, _), _) = setup (as, k1, v1)
      q.refuse()
      as foreach (_.expectNone())
      c.assertNotInvoked()
    }

    "it receives an older grant, it should ignore it" in {
      implicit val (random, scheduler, network) = setupKit()
      val (p, as) = setupThreeAcceptors()
      val c = p.propose (k1, v1) .capture()
      val (Seq (q, _, _), _) = setup (as, k1, v1)
      q.grant()
      as foreach (_.expectNone())
      c.assertNotInvoked()
    }

    "refused, it should restart by asking again" in {
      implicit val (random, scheduler, network) = setupKit()
      val (p, as) = setupThreeAcceptors()
      val c = p.propose (k1, v1) .capture()
      val (_, Seq (q, _, _)) = setup (as, k1, v1)
      q.refuse()
      expectAsk (as, k1, v1)
      as foreach (_.expectNone())
      c.assertNotInvoked()
    }

    "accepted, it should choose the proposed value" in {
      implicit val (random, scheduler, network) = setupKit()
      val (p, as) = setupThreeAcceptors()
      val c = p.propose (k1, v1) .capture()
      val (_, Seq (q1, q2, q3)) = setup (as, k1, v1)
      q1.accept()
      q2.accept()
      c.expectPass (v1)
      as foreach (_.expectChoose (k1, v1))
      q3.accept()
      as foreach (_.expectNone())
    }

    "ignored, it should restart by asking again" in {
      implicit val (random, scheduler, network) = setupKit()
      val (p, as) = setupThreeAcceptors()
      val c = p.propose (k1, v1) .capture()
      setup (as, k1, v1)
      expectAsk (as, k1, v1)
      as foreach (_.expectNone())
      c.assertNotInvoked()
    }

    "ignored twice, it should restart twice" in {
      implicit val (random, scheduler, network) = setupKit()
      val (p, as) = setupThreeAcceptors()
      val c = p.propose (k1, v1) .capture()
      setup (as, k1, v1)
      expectAsk (as, k1, v1)
      expectAsk (as, k1, v1)
      c.assertNotInvoked()
    }

    "ignored while asking, and ignored while proposing, it should restart" in {
      implicit val (random, scheduler, network) = setupKit()
      val (p, as) = setupThreeAcceptors()
      val c = p.propose (k1, v1) .capture()
      expectAsk (as, k1, v1)
      val Seq (q1, q2, _) = expectAsk (as, k1, v1)
      q1.grant()
      q2.grant()
      as map (_.expectPropose (k1, q1.ballot, v1))
      expectAsk (as, k1, v1)
      expectAsk (as, k1, v1)
      c.assertNotInvoked()
    }}

  "The proposer should lead, and when" - {

    "refused, it should restart by asking" in {
      implicit val (random, scheduler, network) = setupKit()
      val (p, as) = setupThreeAcceptors()
      val c = p.lead (k1, v1) .capture()
      val Seq (q, _, _) = as map (_.expectPropose (k1, 0, v1))
      q.refuse()
      expectAsk (as, k1, v1)
      as foreach (_.expectNone())
      c.assertNotInvoked()
    }

    "accepted, it should choose the proposed value" in {
      implicit val (random, scheduler, network) = setupKit()
      val (p, as) = setupThreeAcceptors()
      val c = p.lead (k1, v1) .capture()
      val Seq (q1, q2, q3) = as map (_.expectPropose (k1, 0, v1))
      q1.accept()
      q2.accept()
      c.expectPass (v1)
      as foreach (_.expectChoose (k1, v1))
      q3.accept()
      as foreach (_.expectNone())
    }

    "ignored, it should restart" in {
      implicit val (random, scheduler, network) = setupKit()
      val (p, as) = setupThreeAcceptors()
      val c = p.lead (k1, v1) .capture()
      as map (_.expectPropose (k1, 0, v1))
      expectAsk (as, k1, v1)
      c.assertNotInvoked()
    }}}
