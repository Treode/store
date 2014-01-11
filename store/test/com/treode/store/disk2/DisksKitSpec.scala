package com.treode.store.disk2

import java.nio.file.{Path, Paths}
import scala.language.implicitConversions

import com.treode.async._
import com.treode.async.io.{File, MockFile, StubFile}
import com.treode.buffer.PagedBuffer
import com.treode.pickle.{pickle, unpickle}
import org.scalatest.FreeSpec

class DisksKitSpec extends FreeSpec {

  val big = DiskDriveConfig (30, 13, 1L<<40)

  implicit def pathToString (s: String): Path = Paths.get (s)

  private def mkSuperBlock (gen: Int, disks: Set [String], config: DiskDriveConfig) (
      implicit scheduler: Scheduler) = {
    val file = new StubFile (scheduler)
    val alloc = new SegmentAllocator (config)
    alloc.init()
    val log = new LogWriter (file, alloc, null, null)
    log.init (Callback.ignore)
    SuperBlock (
        BootBlock (gen, disks map (Paths.get (_))),
        config,
        alloc.checkpoint (gen),
        log.checkpoint (gen))
  }

  private class RichStubFile (scheduler: StubScheduler) extends StubFile (scheduler) {

    def readSuperBlock (gen: Int): SuperBlock = {
      val buffer = PagedBuffer (12)
      val pos = if ((gen & 1) == 0) 0 else SuperBlockBytes
      val cb = new CallbackCaptor [Unit]
      fill (buffer, pos, 1, cb)
      scheduler.runTasks()
      cb.passed
      unpickle (SuperBlock.pickle, buffer)
    }

    def expectSuperBlock (block: SuperBlock): Unit =
      expectResult (block) (readSuperBlock (block.boot.gen))

    def writeSuperBlock (block: SuperBlock) {
      val buffer = PagedBuffer (12)
      pickle (SuperBlock.pickle, block, buffer)
      val pos = if ((block.boot.gen & 1) == 0) 0 else SuperBlockBytes
      val cb = new CallbackCaptor [Unit]
      flush (buffer, pos, cb)
      scheduler.runTasks()
      cb.passed
    }

    def destroySuperBlock (gen: Int) {
      val buffer = PagedBuffer (12)
      for (i <- 0 until SuperBlockBytes)
        buffer.writeByte (0)
      val pos = if ((gen & 1) == 0) 0 else SuperBlockBytes
      val cb = new CallbackCaptor [Unit]
      flush (buffer, pos, cb)
      scheduler.runTasks()
      cb.passed
    }}

  private class RichDisksKit (scheduler: StubScheduler) extends DisksKit (scheduler) {

    def assertOpening() = assert (state.isInstanceOf [Opening])
    def assertReady() = assert (state == Ready)
    def assertPanicked() = assert (state.isInstanceOf [Panicked])

    def expectDisks (gen: Int) (paths: Path*) {
      expectResult (paths.size) (disks.size)
      for (path <- paths) {
        val disk = disks.find (_.path == path) .get
      }}

    def attachAndPass (items: (Path, File, DiskDriveConfig)*) {
      val cb = new CallbackCaptor [Unit]
      attach (items, cb)
      scheduler.runTasks()
      cb.passed
      assertReady()
    }

    def attachAndFail [E] (items: (Path, File, DiskDriveConfig)*) (implicit m: Manifest [E]) {
      val cb = new CallbackCaptor [Unit]
      attach (items, cb)
      scheduler.runTasks()
      m.runtimeClass.isInstance (cb.failed)
      if (disks.size == 0)
        assertOpening()
      else
        assertReady()
    }

    def attachAndHold (items: (Path, File, DiskDriveConfig)*): CallbackCaptor [Unit] = {
      val cb = new CallbackCaptor [Unit]
      attach (items, cb)
      cb
    }

    def reattachAndPass (items: (Path, File)*) {
      val cb = new CallbackCaptor [Unit]
      reattach (items, cb)
      scheduler.runTasks()
      cb.passed
      assertReady()
    }

    def reattachAndFail [E] (items: (Path, File)*) (implicit m: Manifest [E]) {
      val cb = new CallbackCaptor [Unit]
      reattach (items, cb)
      scheduler.runTasks()
      m.runtimeClass.isInstance (cb.failed)
      if (m.runtimeClass.isAssignableFrom (classOf [RecoveryCompletedException]))
        assertReady()
      else
        assertPanicked()
    }

    def reattachAndHold (items: (Path, File)*): CallbackCaptor [Unit] = {
      val cb = new CallbackCaptor [Unit]
      reattach (items, cb)
      cb
    }

    def checkpointAndPass() {
      val cb = new CallbackCaptor [Unit]
      checkpoint (cb)
      scheduler.runTasks()
      cb.passed
    }

    def checkpointAndHold(): CallbackCaptor [Unit] = {
      val cb = new CallbackCaptor [Unit]
      checkpoint (cb)
      cb
    }

    def checkpointAndQueue(): CallbackCaptor [Unit] = {
      val cb = new CallbackCaptor [Unit]
      checkpoint (cb)
      scheduler.runTasks()
      assert (!cb.wasInvoked)
      cb
    }}

  "When the DiskSystem is Opening it should" - {

    "attach a new disk" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val kit = new RichDisksKit (scheduler)
      kit.attachAndPass (("a", disk1, big))
      kit.expectDisks (1) ("a")
      disk1.expectSuperBlock (mkSuperBlock (1, Set ("a"), big))
    }

    "attach two new disks" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val disk2 = new RichStubFile (scheduler)
      val kit = new RichDisksKit (scheduler)
      kit.attachAndPass (("a", disk1, big), ("b", disk2, big))
      kit.expectDisks (1) ("a", "b")
      val sb1 = mkSuperBlock (1, Set ("a", "b"), big)
      disk1.expectSuperBlock (sb1)
      disk1.expectSuperBlock (sb1)
    }

    "pass through an attach failure and remain opening" in {
      val scheduler = StubScheduler.random()
      val disk1 = new MockFile
      val kit = new RichDisksKit (scheduler)
      kit.attachAndFail (("a", disk1, big))
      kit.expectDisks (1) ()
    }

    "reattach a disk with only the first superblock" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      disk1.writeSuperBlock (mkSuperBlock (2, Set ("a"), big))
      disk1.destroySuperBlock (1)
      val kit = new RichDisksKit (scheduler)
      kit.reattachAndPass (("a", disk1))
      kit.expectDisks (2) ("a")
    }

    "reattach a disk with only the second superblock" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      disk1.destroySuperBlock (0)
      disk1.writeSuperBlock (mkSuperBlock (1, Set ("a"), big))
      val kit = new RichDisksKit (scheduler)
      kit.reattachAndPass (("a", disk1))
      kit.expectDisks (1) ("a")
    }

    "reattach a disk with the first superblock as newest" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      disk1.writeSuperBlock (mkSuperBlock (4, Set ("a"), big))
      disk1.writeSuperBlock (mkSuperBlock (3, Set ("a"), big))
      val kit = new RichDisksKit (scheduler)
      kit.reattachAndPass (("a", disk1))
      kit.expectDisks (4) ("a")
    }

    "reattach a disk with the second superblock as newest" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      disk1.writeSuperBlock (mkSuperBlock (2, Set ("a"), big))
      disk1.writeSuperBlock (mkSuperBlock (3, Set ("a"), big))
      val kit = new RichDisksKit (scheduler)
      kit.reattachAndPass (("a", disk1))
      kit.expectDisks (3) ("a")
    }

    "reattach two disks with only the first superblock" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val sb1 = mkSuperBlock (2, Set ("a", "b"), big)
      disk1.writeSuperBlock (sb1)
      disk1.destroySuperBlock (1)
      val disk2 = new RichStubFile (scheduler)
      disk2.writeSuperBlock (sb1)
      disk2.destroySuperBlock (1)
      val kit = new RichDisksKit (scheduler)
      kit.reattachAndPass (("a", disk1), ("b", disk2))
      kit.expectDisks (2) ("a", "b")
    }

    "reattach two disks with only the second superblock" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val sb1 = mkSuperBlock (1, Set ("a", "b"), big)
      disk1.destroySuperBlock (0)
      disk1.writeSuperBlock (sb1)
      val disk2 = new RichStubFile (scheduler)
      disk2.destroySuperBlock (0)
      disk2.writeSuperBlock (sb1)
      val kit = new RichDisksKit (scheduler)
      kit.reattachAndPass (("a", disk1), ("b", disk2))
      kit.expectDisks (1) ("a", "b")
    }

    "reattach two disks with first superblock as newest" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val sb1 = mkSuperBlock (4, Set ("a", "b"), big)
      val sb2 = mkSuperBlock (3, Set ("a", "b"), big)
      disk1.writeSuperBlock (sb1)
      disk1.writeSuperBlock (sb2)
      val disk2 = new RichStubFile (scheduler)
      disk2.writeSuperBlock (sb1)
      disk2.writeSuperBlock (sb2)
      val kit = new RichDisksKit (scheduler)
      kit.reattachAndPass (("a", disk1), ("b", disk2))
      kit.expectDisks (4) ("a", "b")
    }

    "reattach two disks with second superblock as newest" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val sb1 = mkSuperBlock (2, Set ("a", "b"), big)
      val sb2 = mkSuperBlock (3, Set ("a", "b"), big)
      disk1.writeSuperBlock (sb1)
      disk1.writeSuperBlock (sb2)
      val disk2 = new RichStubFile (scheduler)
      disk2.writeSuperBlock (sb1)
      disk2.writeSuperBlock (sb2)
      val kit = new RichDisksKit (scheduler)
      kit.reattachAndPass (("a", disk1), ("b", disk2))
      kit.expectDisks (3) ("a", "b")
    }

    "reattach two disks with only first superblock in agreement" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val sb1 = mkSuperBlock (2, Set ("a", "b"), big)
      val sb2 = mkSuperBlock (3, Set ("a", "b"), big)
      disk1.writeSuperBlock (sb1)
      disk1.writeSuperBlock (sb2)
      val disk2 = new RichStubFile (scheduler)
      disk2.writeSuperBlock (sb1)
      disk2.destroySuperBlock (3)
      val kit = new RichDisksKit (scheduler)
      kit.reattachAndPass (("a", disk1), ("b", disk2))
      kit.expectDisks (2) ("a", "b")
    }

    "reattach two disks with only second superblock in agreement" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val sb1 = mkSuperBlock (4, Set ("a", "b"), big)
      val sb2 = mkSuperBlock (3, Set ("a", "b"), big)
      disk1.writeSuperBlock (sb1)
      disk1.writeSuperBlock (sb2)
      val disk2 = new RichStubFile (scheduler)
      disk2.destroySuperBlock (0)
      disk2.writeSuperBlock (sb2)
      val kit = new RichDisksKit (scheduler)
      kit.reattachAndPass (("a", disk1), ("b", disk2))
      kit.expectDisks (3) ("a", "b")
    }

    "panic on reattaching two disks with no superblocks in agreement" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      disk1.writeSuperBlock (mkSuperBlock (2, Set ("a", "b"), big))
      disk1.writeSuperBlock (mkSuperBlock (1, Set ("a", "b"), big))
      val disk2 = new RichStubFile (scheduler)
      disk2.writeSuperBlock (mkSuperBlock (4, Set ("a", "b"), big))
      disk2.writeSuperBlock (mkSuperBlock (3, Set ("a", "b"), big))
      val kit = new RichDisksKit (scheduler)
      kit.reattachAndFail [MultiException] (("a", disk1), ("b", disk2))
    }

    "panic on reattaching two disks with no superblocks fully present" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      disk1.writeSuperBlock (mkSuperBlock (4, Set ("a", "b"), big))
      disk1.destroySuperBlock (3)
      val disk2 = new RichStubFile (scheduler)
      disk2.destroySuperBlock (4)
      disk2.writeSuperBlock (mkSuperBlock (3, Set ("a", "b"), big))
      val kit = new RichDisksKit (scheduler)
      kit.reattachAndFail (("a", disk1), ("b", disk2))
    }

    "queue a checkpoint and complete it with attach" in {
      implicit val scheduler = StubScheduler.random()
      val kit = new RichDisksKit (scheduler)
      val cb = kit.checkpointAndQueue()
      kit.assertOpening()
      val disk1 = new RichStubFile (scheduler)
      kit.attachAndPass (("a", disk1, big))
      cb.passed
      disk1.expectSuperBlock (mkSuperBlock (1, Set ("a"), big))
    }

    "queue a checkpoint and complete it after reattach" in {
      implicit val scheduler = StubScheduler.random()
      val kit = new RichDisksKit (scheduler)
      val cb = kit.checkpointAndQueue()
      kit.assertOpening()
      val disk1 = new RichStubFile (scheduler)
      disk1.writeSuperBlock (mkSuperBlock (1, Set ("a"), big))
      disk1.destroySuperBlock (0)
      kit.reattachAndPass (("a", disk1))
      cb.passed
      disk1.expectSuperBlock (mkSuperBlock (2, Set ("a"), big))
      disk1.expectSuperBlock (mkSuperBlock (1, Set ("a"), big))
    }}

  "When the DiskSystem is Ready it should" - {

    "attach a new disk" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val kit = new RichDisksKit (scheduler)
      kit.attachAndPass (("a", disk1, big))

      val disk2 = new RichStubFile (scheduler)
      kit.attachAndPass (("b", disk2, big))
      kit.expectDisks (2) ("a", "b")
      val sb1 = mkSuperBlock (1, Set ("a"), big)
      disk1.expectSuperBlock (sb1)
      val sb2 = mkSuperBlock (2, Set ("a", "b"), big)
      disk1.expectSuperBlock (sb2)
      disk2.expectSuperBlock (sb2)
    }

    "attach two new disks" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val kit = new RichDisksKit (scheduler)
      kit.attachAndPass (("a", disk1, big))

      val disk2 = new RichStubFile (scheduler)
      val disk3 = new RichStubFile (scheduler)
      kit.attachAndPass (("b", disk2, big), ("c", disk3, big))
      kit.expectDisks (2) ("a", "b", "c")
      val sb1 = mkSuperBlock (1, Set ("a"), big)
      disk1.expectSuperBlock (sb1)
      val sb2 = mkSuperBlock (2, Set ("a", "b", "c"), big)
      disk1.expectSuperBlock (sb2)
      disk2.expectSuperBlock (sb2)
      disk3.expectSuperBlock (sb2)
    }

    "pass through an attach failure and remain ready" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val kit = new RichDisksKit (scheduler)
      kit.attachAndPass (("a", disk1, big))

      val disk2 = new MockFile
      kit.attachAndFail (("b", disk2, big))
      kit.expectDisks (1) ("a")
    }

    "reject reattaching a disk" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val kit = new RichDisksKit (scheduler)
      kit.attachAndPass (("a", disk1, big))

      val disk2 = new MockFile
      kit.reattachAndFail [RecoveryCompletedException] (("b", disk2))
      val sb1 = mkSuperBlock (1, Set ("a"), big)
      disk1.expectSuperBlock (sb1)
    }

    "checkpoint" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val kit = new RichDisksKit (scheduler)
      kit.attachAndPass (("a", disk1, big))

      kit.checkpointAndPass()
      disk1.expectSuperBlock (mkSuperBlock (2, Set ("a"), big))
      disk1.expectSuperBlock (mkSuperBlock (1, Set ("a"), big))
    }

    "pass through a failed checkpoint and panic" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new MockFile
      val kit = new RichDisksKit (scheduler)
      var cb = new CallbackCaptor [Unit]
      kit.attach (Seq (("a", disk1, big)), cb)
      disk1.expectFlush (DiskLeadBytes, 0, 20)
      scheduler.runTasks()
      disk1.completeLast()
      disk1.expectFlush (SuperBlockBytes, 0, 51)
      scheduler.runTasks()
      assert (!cb.wasInvoked)
      disk1.completeLast()
      scheduler.runTasks()
      cb.passed
      kit.expectDisks (1) ("a")
      kit.assertReady()

      cb = new CallbackCaptor [Unit]
      kit.checkpoint (cb)
      disk1.expectFlush (0, 0, 51)
      scheduler.runTasks()
      assert (!cb.wasInvoked)
      disk1.failLast (new Exception)
      scheduler.runTasks()
      cb.failed
      kit.expectDisks (2) ("a")
      kit.assertPanicked()
    }}

  "When the DiskSystem is Attaching it should" - {

    "queue a second attach then complete it after the first attach" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val kit = new RichDisksKit (scheduler)
      kit.attachAndHold (("a", disk1, big))

      val disk2 = new RichStubFile (scheduler)
      kit.attachAndPass (("b", disk2, big))
      kit.expectDisks (2) ("a", "b")
      expectResult (2) (kit.disks.size)
      disk1.expectSuperBlock(mkSuperBlock (1, Set ("a"), big))
      disk1.expectSuperBlock(mkSuperBlock (2, Set ("a", "b"), big))
      disk2.expectSuperBlock(mkSuperBlock (2, Set ("a", "b"), big))
    }

    "reject reattaching a disk" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val kit = new RichDisksKit (scheduler)
      kit.attachAndHold (("a", disk1, big))

      val disk2 = new MockFile
      kit.reattachAndFail [RecoveryCompletedException] (("b", disk2))
      kit.expectDisks (1) ("a")
      disk1.expectSuperBlock (mkSuperBlock (1, Set ("a"), big))
    }

    "queue a checkpoint and complete it with the attach" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val kit = new RichDisksKit (scheduler)
      kit.attachAndHold (("a", disk1, big))

      kit.checkpointAndPass()
      kit.expectDisks (1) ("a")
      disk1.expectSuperBlock (mkSuperBlock (1, Set ("a"), big))
    }}

  "When the DiskSystem is Reattaching it should" - {

    "queue an attach then complete it after the reattach" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      disk1.writeSuperBlock( mkSuperBlock (1, Set ("a"), big))
      val kit = new RichDisksKit (scheduler)
      kit.reattachAndHold (("a", disk1))

      val disk2 = new RichStubFile (scheduler)
      kit.attachAndPass (("b", disk2, big))
      kit.expectDisks (2) ("a", "b")
      disk1.expectSuperBlock(mkSuperBlock (1, Set ("a"), big))
      disk1.expectSuperBlock(mkSuperBlock (2, Set ("a", "b"), big))
      disk2.expectSuperBlock(mkSuperBlock (2, Set ("a", "b"), big))
    }

    "reject reattaching a disk" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      disk1.writeSuperBlock( mkSuperBlock (1, Set ("a"), big))
      val kit = new RichDisksKit (scheduler)
      kit.reattachAndHold (("a", disk1))

      val disk2 = new MockFile
      kit.reattachAndFail [RecoveryCompletedException] (("b", disk2))
      kit.expectDisks (1) ("a")
      disk1.expectSuperBlock (mkSuperBlock (1, Set ("a"), big))
    }

    "queue a checkpoint and complete it after the reattach" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      disk1.writeSuperBlock( mkSuperBlock (1, Set ("a"), big))
      val kit = new RichDisksKit (scheduler)
      kit.reattachAndHold (("a", disk1))

      kit.checkpointAndPass()
      kit.expectDisks (2) ("a")
      disk1.expectSuperBlock (mkSuperBlock (1, Set ("a"), big))
      disk1.expectSuperBlock (mkSuperBlock (2, Set ("a"), big))
    }
  }

  "When the DiskSystem is Checkpointing it should" - {

    "queue an attach then complete it after the checkpoint" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val kit = new RichDisksKit (scheduler)
      kit.attachAndPass (("a", disk1, big))
      kit.checkpointAndHold()

      val disk2 = new RichStubFile (scheduler)
      kit.attachAndPass (("b", disk2, big))
      kit.expectDisks (3) ("a", "b")
      disk1.expectSuperBlock(mkSuperBlock (2, Set ("a"), big))
      disk1.expectSuperBlock(mkSuperBlock (3, Set ("a", "b"), big))
      disk2.expectSuperBlock(mkSuperBlock (3, Set ("a", "b"), big))
    }

    "reject reattaching a disk" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val kit = new RichDisksKit (scheduler)
      kit.attachAndPass (("a", disk1, big))
      kit.checkpointAndHold()

      val disk2 = new MockFile
      kit.reattachAndFail [RecoveryCompletedException] (("b", disk2))
      kit.expectDisks (2) ("a")
      disk1.expectSuperBlock (mkSuperBlock (1, Set ("a"), big))
      disk1.expectSuperBlock (mkSuperBlock (2, Set ("a"), big))
    }

    "queue a second checkpoint and complete it with the first checkpoint" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val kit = new RichDisksKit (scheduler)
      kit.attachAndPass (("a", disk1, big))
      kit.checkpointAndHold()

      kit.checkpointAndPass()
      kit.expectDisks (2) ("a")
      disk1.expectSuperBlock (mkSuperBlock (1, Set ("a"), big))
      disk1.expectSuperBlock (mkSuperBlock (2, Set ("a"), big))
    }}}
