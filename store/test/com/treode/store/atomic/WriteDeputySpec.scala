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

package com.treode.store.atomic

import scala.util.Random

import com.treode.async.Async
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.cluster.{EphemeralPort, Peer, PortId}
import com.treode.cluster.stubs.{MessageCaptor, StubNetwork, StubPeer}
import com.treode.store.{StoreTestTools, TxClock, TxId, WriteOp}
import org.scalatest.FreeSpec

import StoreTestTools._
import WriteOp._
import WriteResponse._

class WriteDeputySpec extends FreeSpec {

  private implicit class RicStubPeer (peer: StubPeer) {

    def prepare (xid: TxId, ct: TxClock, op: WriteOp) (implicit network: StubNetwork) =
      network.request (WriteDeputy.prepare, (1, xid, ct, Seq (op)), peer)
  }

  val xid1 = TxId (0x5778D6B9EA991FC9L, 0)
  val op1 = Update (0xF5D1800B92307763L, 0x46795104EEF340F0L, 0xFB0FF6B8)

  "The WriteDeputy should" - {

    "work" in {
      implicit val (random, scheduler, network) = newKit()
      val h = StubAtomicHost .install() .expectPass()
      h.prepare (xid1, 0, op1) .expectPass (Prepared (0x0))
    }}}
