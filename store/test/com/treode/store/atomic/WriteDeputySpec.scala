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
import com.treode.async.implicits._
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

    def isOpen (xid: TxId): Boolean =
      deputy (xid) .state.isInstanceOf [WriteDeputy#Open]

    def assertOpen (xid: TxId): Unit =
      assert (isOpen (xid))

    def isPreparing (xid: TxId): Boolean =
      deputy (xid) .state.isInstanceOf [WriteDeputy#Preparing]

    def assertPreparing (xid: TxId): Unit =
      assert (isPreparing (xid))

    def isRecording (xid: TxId): Boolean =
      deputy (xid) .state.isInstanceOf [WriteDeputy#Recording]

    def assertRecording (xid: TxId): Unit =
      assert (isRecording (xid))

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
  val xid3 = TxId (0xE1FFF05DCF0A31EAL, 0)
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
    StubAtomicHost.boot (h.localId, h.drive) .expectPass()
  }

  "When the WriteDeputy is" - {

    "is preparing, it should" - {

      // Write a value to create a condition that the write under test must meet (xid3)
      // Prepare a write to block the write under test (xid2)
      // Start the write under test; check that it is preparing (xid1)
      def boot (ct: TxClock) (
          implicit r: Random, s: StubScheduler, n: StubNetwork, c: StoreTestConfig) = {
        val drive = new StubDiskDrive
        val h = StubAtomicHost.boot (h1, drive) .expectPass()
        h.setAtlas (settled (h))
        h.write (xid3, 0, Update (t1, k1, v1)) .expectPass (1: TxClock)
        h.prepare (xid2, 1, Update (t1, k1, v1)) .expectPass (Prepared (1))
        val cb = h.prepare (xid1, ct, Update (t1, k1, v1)) .capture()
        cb.expectNotInvoked()
        h.assertPreparing (xid1)
        (h, cb)
      }

      "ignore a subsequent prepare" in {
        implicit val (random, scheduler, network, config) = setup()
        val (h, cb1) = boot (1)
        val cb2 = h.prepare (xid1, 1, Update (t1, k1, v1)) .capture()
        cb1.expectNotInvoked()
        cb2.expectNotInvoked()
        h.assertPreparing (xid1)
      }

      "ignore a subsequent prepare on the blocking write" in {
        implicit val (random, scheduler, network, config) = setup()
        val (h, cb) = boot (1)
        h.prepare (xid2, 1, Update (t1, k1, v1)) .expectPass (Prepared (1))
        cb.expectNotInvoked()
        h.assertPreparing (xid1)
      }

      "prepare after the blocking write aborts" in {
        implicit val (random, scheduler, network, config) = setup()
        val (h, cb) = boot (1)
        h.abort (xid2) .expectPass (Aborted)
        cb.expectPass (Prepared (1))
        h.assertPrepared (xid1)
      }

      "advance after the blocking write aborts" in {
        implicit val (random, scheduler, network, config) = setup()
        val (h, cb) = boot (0)
        h.abort (xid2) .expectPass (Aborted)
        cb.expectPass (Advance)
        h.assertDeliberating (xid1)
      }

      "prepare after the blocking write commits" in {
        implicit val (random, scheduler, network, config) = setup()
        val (h, cb) = boot (2)
        h.commit (xid2, 2) .expectPass (Committed)
        cb.expectPass (Prepared (2))
        h.assertPrepared (xid1)
      }

      "advance after the blocking write commits" in {
        implicit val (random, scheduler, network, config) = setup()
        val (h, cb) = boot (1)
        h.commit (xid2, 2) .expectPass (Committed)
        cb.expectPass (Advance)
        h.assertDeliberating (xid1)
      }

      "commit" in {
        implicit val (random, scheduler, network, config) = setup()
        val (h, cb) = boot (1)
        h.commit (xid1, 2) .expectPass (Committed)
        h.assertCommitted (xid1)
      }

      "abort" in {
        implicit val (random, scheduler, network, config) = setup()
        val (h, cb) = boot (1)
        h.abort (xid1) .expectPass (Aborted)
        h.assertAborted (xid1)
      }

      "recover to open" in {
        implicit val (random, scheduler, network, config) = setup()
        var (h, cb) = boot (1)
        h = reboot (h)
        h.assertOpen (xid1)
      }}

    "is recording, it should" - {

      // Write a value to create a condition that the write under test must meet (xid2)
      // Stop the disk drive to hold the write log
      // Start the write under test; check that it is recording (xid1)
      def boot (ct: TxClock) (
          implicit r: Random, s: StubScheduler, n: StubNetwork, c: StoreTestConfig) = {
        val drive = new StubDiskDrive
        val h = StubAtomicHost.boot (h1, drive) .expectPass()
        h.setAtlas (settled (h))
        h.write (xid2, 0, Update (t1, k1, v1)) .expectPass (1: TxClock)
        drive.stop = true
        val cb = h.prepare (xid1, ct, Update (t1, k1, v1)) .capture()
        cb.expectNotInvoked()
        h.assertRecording (xid1)
        drive.stop = false
        (h, cb)
      }

      "ignore a subsequent prepare" in {
        implicit val (random, scheduler, network, config) = setup()
        val (h, cb1) = boot (1)
        val cb2 = h.prepare (xid1, 1, Update (t1, k1, v1)) .capture()
        cb1.expectNotInvoked()
        cb2.expectNotInvoked()
        h.assertRecording (xid1)
      }

      "prepare after the log entry is recorded" in {
        implicit val (random, scheduler, network, config) = setup()
        val (h, cb) = boot (1)
        h.drive.last.pass (())
        cb.expectPass (Prepared (1))
        h.assertPrepared (xid1)
      }

      "commit" in {
        implicit val (random, scheduler, network, config) = setup()
        val (h, _) = boot (1)
        val cb = h.commit (xid1, 2) .capture()
        cb.expectNotInvoked()
        h.drive.last.pass (())
        cb.expectPass (Committed)
        h.assertCommitted (xid1)
      }

      "abort" in {
        implicit val (random, scheduler, network, config) = setup()
        val (h, _) = boot (1)
        val cb = h.abort (xid1) .capture()
        cb.expectNotInvoked()
        h.drive.last.pass (())
        cb.expectPass (Aborted)
        h.assertAborted (xid1)
      }

      "recover to open" in {
        implicit val (random, scheduler, network, config) = setup()
        var (h, cb) = boot (1)
        h = reboot (h)
        h.assertOpen (xid1)
      }}

    "is prepared, it should" - {

      // Write a value to create a condition that the write under test must meet (xid2)
      // Start the write under test; check that it is prepared (xid1)
      def boot () (implicit r: Random, s: StubScheduler, n: StubNetwork, c: StoreTestConfig) = {
        val drive = new StubDiskDrive
        val h = StubAtomicHost.boot (h1, drive) .expectPass()
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

      "recover to prepared" in {
        implicit val (random, scheduler, network, config) = setup()
        var h = boot()
        h = reboot (h)
        h.prepare (xid1, 1, Update (t1, k1, v1)) .expectPass (Prepared (1))
        h.assertPrepared (xid1)
      }}

    "is deliberating, should" - {

      // Write a value to create a condition that the write under test fails to meet (xid2)
      // Start the write under test; check that it is deliberating (xid1)
      def boot () (implicit r: Random, s: StubScheduler, n: StubNetwork, c: StoreTestConfig) = {
        val drive = new StubDiskDrive
        val h = StubAtomicHost.boot (h1, drive) .expectPass()
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

      "reboot to deliberating" in {
        implicit val (random, scheduler, network, config) = setup()
        var h = boot()
        h = reboot (h)
        h.prepare (xid1, 0, Update (t1, k1, v1)) .expectPass (Advance)
        h.assertDeliberating (xid1)
      }}

    "is tardy, should" - {

      // Write a value to create a condition that the write under test must meet (xid2)
      // Start the write under test; check that it is tardy (xid1)
      def boot () (implicit r: Random, s: StubScheduler, n: StubNetwork, c: StoreTestConfig) = {
        val drive = new StubDiskDrive
        val h = StubAtomicHost.boot (h1, drive) .expectPass()
        h.setAtlas (settled (h))
        h.write (xid2, 0, Update (t1, k1, v1)) .expectPass (1: TxClock)
        val cb = h.commit (xid1, 2) .capture()
        cb.expectNotInvoked()
        h.assertTardy (xid1)
        (h, cb)
      }

      "prepare" in {
        implicit val (random, scheduler, network, config) = setup()
        val (h, cb) = boot()
        h.prepare (xid1, 1, Update (t1, k1, v1)) .expectPass (Committed)
        h.assertCommitted (xid1)
      }

      "ignore a subsequent commit" in {
        implicit val (random, scheduler, network, config) = setup()
        val (h, cb1) = boot()
        val cb2 = h.commit (xid1, 2) .capture()
        cb1.expectNotInvoked()
        cb2.expectNotInvoked()
        h.assertTardy (xid1)
      }}}}
