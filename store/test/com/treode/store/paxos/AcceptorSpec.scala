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

import com.treode.async.Async
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.cluster.{EphemeralPort, Peer, PortId}
import com.treode.cluster.stubs.{MessageCaptor, StubNetwork, StubPeer}
import com.treode.store.{Bytes, StoreTestTools, TxClock}
import org.scalatest.FreeSpec

import StoreTestTools._

class AcceptorSpec extends FreeSpec {

  private implicit class RicStubPeer (peer: StubPeer) {

    def ask (key: Long, ballot: Long, default: Int, from: MessageCaptor): Unit =
      from.send (Acceptor.ask, peer) (1, key, ballot, default)
  }

  private case class Grant (key: Long, ballot: Long, proposed: Option [(Long, Int)])

  private implicit class RichMessageCaptor (c: MessageCaptor) {

    def expectGrant (peer: StubPeer) (implicit s: StubScheduler): Grant = {
      val ((key, b1, proposed), from) = c.expect (Proposer.grant)
      assertResult (peer.localId) (from.id)
      proposed match {
        case Some ((b2, value)) => Grant (key.long, b1, Some (b2.number, value.int))
        case None => Grant (key.long, b1, None)
      }}}

  val k1 = 0xB91DBC0E0EE50880L
  val v1 = 0xCCEA074C

  "The Acceptor should" - {

    "work" in {
      implicit val (random, scheduler, network) = newKit()
      val captor = MessageCaptor.install()
      val h = StubPaxosHost .install() .expectPass()
      h.ask (k1, 1, v1, captor)
      assertResult (Grant (k1, 1, None)) (captor.expectGrant (h))
    }}}
