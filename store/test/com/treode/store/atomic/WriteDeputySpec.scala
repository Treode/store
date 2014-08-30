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
import com.treode.disk.stubs.StubDiskDrive
import com.treode.store._
import org.scalatest.FreeSpec

import StoreTestTools._
import WriteOp._
import WriteResponse._

class WriteDeputySpec extends FreeSpec {

  private implicit class RicStubPeer (peer: StubPeer) {

    def prepare (xid: TxId, ct: TxClock, op: WriteOp) (implicit network: StubNetwork) =
      network.request (WriteDeputy.prepare, (1, xid, ct, Seq (op)), peer)

    def commit (xid: TxId, wt: TxClock) (implicit network: StubNetwork) =
      network.request (WriteDeputy.commit, (xid, wt), peer)

    def abort (xid: TxId) (implicit network: StubNetwork) =
      network.request (WriteDeputy.abort, (xid), peer)
  }

  private implicit class RichAtomicHost (host: StubAtomicHost) {

    def deputy (xid: TxId): WriteDeputy =
      host.atomic.writers.get (xid)

    def isPrepared (xid: TxId): Boolean =
      deputy (xid) .state.isInstanceOf [WriteDeputy#Prepared]

    def assertPrepared (xid: TxId): Unit =
      assert (isPrepared (xid))

    def isDeliberating (xid: TxId): Boolean =
      deputy (xid) .state.isInstanceOf [WriteDeputy#Deliberating]

    def assertDeliberating (xid: TxId): Unit =
      assert (isDeliberating (xid))

    def isTardy (xid: TxId): Boolean =
      deputy (xid) .state.isInstanceOf [WriteDeputy#Tardy]

    def assertTardy (xid: TxId): Unit =
      assert (isTardy (xid))

    def isCommitted (xid: TxId): Boolean =
      deputy (xid) .state.isInstanceOf [WriteDeputy#Committed]

    def assertCommitted (xid: TxId): Unit =
      assert (isCommitted (xid))

    def isAborted (xid: TxId): Boolean =
      deputy (xid) .state.isInstanceOf [WriteDeputy#Aborted]

    def assertAborted (xid: TxId): Unit =
      assert (isAborted (xid))
  }

  val xid1 = TxId (0x5778D6B9EA991FC9L, 0)
  val xid2 = TxId (0x3DBE3E2426306B4AL, 0)
  val h1 = 0x14181214A2030C7EL
  val t1 = 0xF5D1800B92307763L
  val k1 = 0x46795104EEF340F0L
  val v1 = 0xFB0FF6B8
  val v2 = 0xFD10F8D9

  private def setup() = {
    implicit val (random, scheduler, network) = newKit()
    implicit val config = StoreTestConfig()
    (random, scheduler, network, config)
  }

  private def reboot (h: StubAtomicHost) (
      implicit r: Random, s: StubScheduler, n: StubNetwork, c: StoreTestConfig) = {
    h.shutdown()
    StubAtomicHost .boot (h.localId, h.drive, false) .expectPass()
  }

  "The WriteDeputy when" - {

    "updating where the condition is met, and" - {

      "is open, should" - {

        def boot () (implicit r: Random, s: StubScheduler, n: StubNetwork, c: StoreTestConfig) = {
          val drive = new StubDiskDrive
          val h = StubAtomicHost .boot (h1, drive, true) .expectPass()
          h.setAtlas (settled (h))
          h.write (xid2, 0, Update (t1, k1, v1)) .expectPass (1: TxClock)
          h
        }

        "prepare" in {
          implicit val (random, scheduler, network, config) = setup()
          val h = boot()
          h.prepare (xid1, 1, Update (t1, k1, v1)) .expectPass (Prepared (1))
          h.assertPrepared (xid1)
        }

        "commit" in {
          implicit val (random, scheduler, network, config) = setup()
          val h = boot()
          val c = h.commit (xid1, 1) .capture()
          scheduler.run()
          h.assertTardy (xid1)
          c.assertNotInvoked()
        }

        "abort" in {
          implicit val (random, scheduler, network, config) = setup()
          val h = boot()
          h.abort (xid1) .expectPass (Aborted)
          h.assertAborted (xid1)
        }}

      "is prepared, should" - {

        def boot () (implicit r: Random, s: StubScheduler, n: StubNetwork, c: StoreTestConfig) = {
          val drive = new StubDiskDrive
          val h = StubAtomicHost .boot (h1, drive, true) .expectPass()
          h.setAtlas (settled (h))
          h.write (xid2, 0, Update (t1, k1, v1)) .expectPass (1: TxClock)
          h.prepare (xid1, 1, Update (t1, k1, v1)) .expectPass (Prepared (1))
          h.assertPrepared (xid1)
          h
        }

        "prepare" in {
          implicit val (random, scheduler, network, config) = setup()
          val h = boot()
          h.prepare (xid1, 1, Update (t1, k1, v1)) .expectPass (Prepared (1))
          h.assertPrepared (xid1)
        }

        "commit" in {
          implicit val (random, scheduler, network, config) = setup()
          val h = boot()
          h.commit (xid1, 2) .expectPass (Committed)
          h.assertCommitted (xid1)
        }

        "abort" in {
          implicit val (random, scheduler, network, config) = setup()
          val h = boot()
          h.abort (xid1) .expectPass (Aborted)
          h.assertAborted (xid1)
        }

        "reboot and prepare" in {
          implicit val (random, scheduler, network, config) = setup()
          var h = boot()
          h = reboot (h)
          h.prepare (xid1, 1, Update (t1, k1, v1)) .expectPass (Prepared (1))
          h.assertPrepared (xid1)
        }}}

      "is tardy, should" - {

        def boot () (implicit r: Random, s: StubScheduler, n: StubNetwork, c: StoreTestConfig) = {
          val drive = new StubDiskDrive
          val h = StubAtomicHost .boot (h1, drive, true) .expectPass()
          h.setAtlas (settled (h))
          val captor = h.commit (xid1, 2) .capture()
          s.run()
          h.assertTardy (xid1)
          captor.assertNotInvoked()
          h
        }

        "prepare" in {
          implicit val (random, scheduler, network, config) = setup()
          val h = boot()
          h.prepare (xid1, 1, Update (t1, k1, v1)) .capture()
          scheduler.run()
          h.assertCommitted (xid1)
        }

        "commit" in {
          implicit val (random, scheduler, network, config) = setup()
          val h = boot()
          val c = h.commit (xid1, 2) .capture()
          scheduler.run()
          h.assertTardy (xid1)
          c.assertNotInvoked()
        }}

    "updating where the condition is unmet, and" - {

      "is open, should" - {

        def boot () (implicit r: Random, s: StubScheduler, n: StubNetwork, c: StoreTestConfig) = {
          val drive = new StubDiskDrive
          val h = StubAtomicHost .boot (h1, drive, true) .expectPass()
          h.setAtlas (settled (h))
          h.write (xid2, 0, Update (t1, k1, v1)) .expectPass (1: TxClock)
          h
        }

        "prepare" in {
          implicit val (random, scheduler, network, config) = setup()
          val h = boot()
          h.prepare (xid1, 0, Update (t1, k1, v1)) .expectPass (Advance)
          h.assertDeliberating (xid1)
        }}

      "is deliberating, should" - {

        def boot () (implicit r: Random, s: StubScheduler, n: StubNetwork, c: StoreTestConfig) = {
          val drive = new StubDiskDrive
          val h = StubAtomicHost .boot (h1, drive, true) .expectPass()
          h.setAtlas (settled (h))
          h.write (xid2, 0, Update (t1, k1, v1)) .expectPass (1: TxClock)
          h.prepare (xid1, 0, Update (t1, k1, v1)) .expectPass (Advance)
          h.assertDeliberating (xid1)
          h
        }

        "prepare" in {
          implicit val (random, scheduler, network, config) = setup()
          val h = boot()
          h.prepare (xid1, 0, Update (t1, k1, v1)) .expectPass (Advance)
          h.assertDeliberating (xid1)
        }

        "commit" in {
          implicit val (random, scheduler, network, config) = setup()
          val h = boot()
          h.commit (xid1, 2) .expectPass (Committed)
          h.assertCommitted (xid1)
        }

        "abort" in {
          implicit val (random, scheduler, network, config) = setup()
          val h = boot()
          h.abort (xid1) .expectPass (Aborted)
          h.assertAborted (xid1)
        }

        "reboot and prepare" in {
          implicit val (random, scheduler, network, config) = setup()
          var h = boot()
          h = reboot (h)
          h.prepare (xid1, 0, Update (t1, k1, v1)) .expectPass (Advance)
          h.assertDeliberating (xid1)
        }
      }

      "is tardy, should" - {

        def boot () (implicit r: Random, s: StubScheduler, n: StubNetwork, c: StoreTestConfig) = {
          val drive = new StubDiskDrive
          val h = StubAtomicHost .boot (h1, drive, true) .expectPass()
          h.setAtlas (settled (h))
          h.write (xid2, 0, Update (t1, k1, v1)) .expectPass (1: TxClock)
          val captor = h.commit (xid1, 2) .capture()
          s.run()
          h.assertTardy (xid1)
          captor.assertNotInvoked()
          h
        }

        "prepare" in {
          implicit val (random, scheduler, network, config) = setup()
          val h = boot()
          h.prepare (xid1, 0, Update (t1, k1, v1)) .capture()
          scheduler.run()
          h.assertCommitted (xid1)
        }

        "commit" in {
          implicit val (random, scheduler, network, config) = setup()
          val h = boot()
          val c = h.commit (xid1, 2) .capture()
          scheduler.run()
          h.assertTardy (xid1)
          c.assertNotInvoked()
        }}}}}
