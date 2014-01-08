package com.treode.store.disk2

import java.nio.file.{Path, Paths}
import scala.language.implicitConversions

import com.treode.async._
import com.treode.async.io.{File, MockFile, StubFile}
import com.treode.buffer.PagedBuffer
import com.treode.pickle.{pickle, unpickle}
import org.scalatest.FreeSpec

class DiskSystemSpec extends FreeSpec {

  val big = DiskConfig (30, 20, 13, 1L<<40)

  implicit def pathToString (s: String): Path = Paths.get (s)

  private def mkSuperBlock (gen: Int, disks: Set [String], config: DiskConfig) = {
    val free = new Allocator (config)
    free.init()
    SuperBlock (BootBlock (gen, disks map (Paths.get (_))), config, free.checkpoint (gen))
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

  private class RichDiskSystem (scheduler: StubScheduler) extends DiskSystem (scheduler, null) {

    def assertOpening() = assert (state.isInstanceOf [Opening])
    def assertReady() = assert (state == Ready)
    def assertPanicked() = assert (state.isInstanceOf [Panicked])

    def expectDisks (gen: Int) (paths: Path*) {
      expectResult (paths.size) (disks.size)
      for (path <- paths) {
        val disk = disks.find (_.path == path) .get
        expectResult (gen) (disk.free.gen)
      }}

    def attachAndPass (items: (Path, File, DiskConfig)*) {
      val cb = new CallbackCaptor [Unit]
      _attach (items, cb)
      scheduler.runTasks()
      cb.passed
      assertReady()
    }

    def attachAndFail [E] (items: (Path, File, DiskConfig)*) (implicit m: Manifest [E]) {
      val cb = new CallbackCaptor [Unit]
      _attach (items, cb)
      scheduler.runTasks()
      m.runtimeClass.isInstance (cb.failed)
      if (disks.size == 0)
        assertOpening()
      else
        assertReady()
    }

    def attachAndHold (items: (Path, File, DiskConfig)*): CallbackCaptor [Unit] = {
      val cb = new CallbackCaptor [Unit]
      _attach (items, cb)
      cb
    }

    def reattachAndPass (items: (Path, File)*) {
      val cb = new CallbackCaptor [Unit]
      _reattach (items, cb)
      scheduler.runTasks()
      cb.passed
      assertReady()
    }

    def reattachAndFail [E] (items: (Path, File)*) (implicit m: Manifest [E]) {
      val cb = new CallbackCaptor [Unit]
      _reattach (items, cb)
      scheduler.runTasks()
      m.runtimeClass.isInstance (cb.failed)
      if (m.runtimeClass.isAssignableFrom (classOf [RecoveryCompletedException]))
        assertReady()
      else
        assertPanicked()
    }

    def reattachAndHold (items: (Path, File)*): CallbackCaptor [Unit] = {
      val cb = new CallbackCaptor [Unit]
      _reattach (items, cb)
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
      val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val sys = new RichDiskSystem (scheduler)
      sys.attachAndPass (("a", disk1, big))
      sys.expectDisks (1) ("a")
      disk1.expectSuperBlock (mkSuperBlock (1, Set ("a"), big))
    }

    "attach two new disks" in {
      val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val disk2 = new RichStubFile (scheduler)
      val sys = new RichDiskSystem (scheduler)
      sys.attachAndPass (("a", disk1, big), ("b", disk2, big))
      sys.expectDisks (1) ("a", "b")
      val sb1 = mkSuperBlock (1, Set ("a", "b"), big)
      disk1.expectSuperBlock (sb1)
      disk1.expectSuperBlock (sb1)
    }

    "pass through an attach failure and remain opening" in {
      val scheduler = StubScheduler.random()
      val disk1 = new MockFile
      val sys = new RichDiskSystem (scheduler)
      sys.attachAndFail (("a", disk1, big))
      sys.expectDisks (1) ()
    }

    "reattach a disk with only the first superblock" in {
      val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      disk1.writeSuperBlock (mkSuperBlock (2, Set ("a"), big))
      disk1.destroySuperBlock (1)
      val sys = new RichDiskSystem (scheduler)
      sys.reattachAndPass (("a", disk1))
      sys.expectDisks (2) ("a")
    }

    "reattach a disk with only the second superblock" in {
      val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      disk1.destroySuperBlock (0)
      disk1.writeSuperBlock (mkSuperBlock (1, Set ("a"), big))
      val sys = new RichDiskSystem (scheduler)
      sys.reattachAndPass (("a", disk1))
      sys.expectDisks (1) ("a")
    }

    "reattach a disk with the first superblock as newest" in {
      val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      disk1.writeSuperBlock (mkSuperBlock (4, Set ("a"), big))
      disk1.writeSuperBlock (mkSuperBlock (3, Set ("a"), big))
      val sys = new RichDiskSystem (scheduler)
      sys.reattachAndPass (("a", disk1))
      sys.expectDisks (4) ("a")
    }

    "reattach a disk with the second superblock as newest" in {
      val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      disk1.writeSuperBlock (mkSuperBlock (2, Set ("a"), big))
      disk1.writeSuperBlock (mkSuperBlock (3, Set ("a"), big))
      val sys = new RichDiskSystem (scheduler)
      sys.reattachAndPass (("a", disk1))
      sys.expectDisks (3) ("a")
    }

    "reattach two disks with only the first superblock" in {
      val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val sb1 = mkSuperBlock (2, Set ("a", "b"), big)
      disk1.writeSuperBlock (sb1)
      disk1.destroySuperBlock (1)
      val disk2 = new RichStubFile (scheduler)
      disk2.writeSuperBlock (sb1)
      disk2.destroySuperBlock (1)
      val sys = new RichDiskSystem (scheduler)
      sys.reattachAndPass (("a", disk1), ("b", disk2))
      sys.expectDisks (2) ("a", "b")
    }

    "reattach two disks with only the second superblock" in {
      val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val sb1 = mkSuperBlock (1, Set ("a", "b"), big)
      disk1.destroySuperBlock (0)
      disk1.writeSuperBlock (sb1)
      val disk2 = new RichStubFile (scheduler)
      disk2.destroySuperBlock (0)
      disk2.writeSuperBlock (sb1)
      val sys = new RichDiskSystem (scheduler)
      sys.reattachAndPass (("a", disk1), ("b", disk2))
      sys.expectDisks (1) ("a", "b")
    }

    "reattach two disks with first superblock as newest" in {
      val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val sb1 = mkSuperBlock (4, Set ("a", "b"), big)
      val sb2 = mkSuperBlock (3, Set ("a", "b"), big)
      disk1.writeSuperBlock (sb1)
      disk1.writeSuperBlock (sb2)
      val disk2 = new RichStubFile (scheduler)
      disk2.writeSuperBlock (sb1)
      disk2.writeSuperBlock (sb2)
      val sys = new RichDiskSystem (scheduler)
      sys.reattachAndPass (("a", disk1), ("b", disk2))
      sys.expectDisks (4) ("a", "b")
    }

    "reattach two disks with second superblock as newest" in {
      val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val sb1 = mkSuperBlock (2, Set ("a", "b"), big)
      val sb2 = mkSuperBlock (3, Set ("a", "b"), big)
      disk1.writeSuperBlock (sb1)
      disk1.writeSuperBlock (sb2)
      val disk2 = new RichStubFile (scheduler)
      disk2.writeSuperBlock (sb1)
      disk2.writeSuperBlock (sb2)
      val sys = new RichDiskSystem (scheduler)
      sys.reattachAndPass (("a", disk1), ("b", disk2))
      sys.expectDisks (3) ("a", "b")
    }

    "reattach two disks with only first superblock in agreement" in {
      val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val sb1 = mkSuperBlock (2, Set ("a", "b"), big)
      val sb2 = mkSuperBlock (3, Set ("a", "b"), big)
      disk1.writeSuperBlock (sb1)
      disk1.writeSuperBlock (sb2)
      val disk2 = new RichStubFile (scheduler)
      disk2.writeSuperBlock (sb1)
      disk2.destroySuperBlock (3)
      val sys = new RichDiskSystem (scheduler)
      sys.reattachAndPass (("a", disk1), ("b", disk2))
      sys.expectDisks (2) ("a", "b")
    }

    "reattach two disks with only second superblock in agreement" in {
      val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val sb1 = mkSuperBlock (4, Set ("a", "b"), big)
      val sb2 = mkSuperBlock (3, Set ("a", "b"), big)
      disk1.writeSuperBlock (sb1)
      disk1.writeSuperBlock (sb2)
      val disk2 = new RichStubFile (scheduler)
      disk2.destroySuperBlock (0)
      disk2.writeSuperBlock (sb2)
      val sys = new RichDiskSystem (scheduler)
      sys.reattachAndPass (("a", disk1), ("b", disk2))
      sys.expectDisks (3) ("a", "b")
    }

    "panic on reattaching two disks with no superblocks in agreement" in {
      val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      disk1.writeSuperBlock (mkSuperBlock (2, Set ("a", "b"), big))
      disk1.writeSuperBlock (mkSuperBlock (1, Set ("a", "b"), big))
      val disk2 = new RichStubFile (scheduler)
      disk2.writeSuperBlock (mkSuperBlock (4, Set ("a", "b"), big))
      disk2.writeSuperBlock (mkSuperBlock (3, Set ("a", "b"), big))
      val sys = new RichDiskSystem (scheduler)
      sys.reattachAndFail [MultiException] (("a", disk1), ("b", disk2))
    }

    "panic on reattaching two disks with no superblocks fully present" in {
      val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      disk1.writeSuperBlock (mkSuperBlock (4, Set ("a", "b"), big))
      disk1.destroySuperBlock (3)
      val disk2 = new RichStubFile (scheduler)
      disk2.destroySuperBlock (4)
      disk2.writeSuperBlock (mkSuperBlock (3, Set ("a", "b"), big))
      val sys = new RichDiskSystem (scheduler)
      sys.reattachAndFail (("a", disk1), ("b", disk2))
    }

    "queue a checkpoint and complete it with attach" in {
      val scheduler = StubScheduler.random()
      val sys = new RichDiskSystem (scheduler)
      val cb = sys.checkpointAndQueue()
      sys.assertOpening()
      val disk1 = new RichStubFile (scheduler)
      sys.attachAndPass (("a", disk1, big))
      cb.passed
      disk1.expectSuperBlock (mkSuperBlock (1, Set ("a"), big))
    }

    "queue a checkpoint and complete it after reattach" in {
      val scheduler = StubScheduler.random()
      val sys = new RichDiskSystem (scheduler)
      val cb = sys.checkpointAndQueue()
      sys.assertOpening()
      val disk1 = new RichStubFile (scheduler)
      disk1.writeSuperBlock (mkSuperBlock (1, Set ("a"), big))
      disk1.destroySuperBlock (0)
      sys.reattachAndPass (("a", disk1))
      cb.passed
      disk1.expectSuperBlock (mkSuperBlock (2, Set ("a"), big))
      disk1.expectSuperBlock (mkSuperBlock (1, Set ("a"), big))
    }}

  "When the DiskSystem is Ready it should" - {

    "attach a new disk" in {
      val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val sys = new RichDiskSystem (scheduler)
      sys.attachAndPass (("a", disk1, big))

      val disk2 = new RichStubFile (scheduler)
      sys.attachAndPass (("b", disk2, big))
      sys.expectDisks (2) ("a", "b")
      val sb1 = mkSuperBlock (1, Set ("a"), big)
      disk1.expectSuperBlock (sb1)
      val sb2 = mkSuperBlock (2, Set ("a", "b"), big)
      disk1.expectSuperBlock (sb2)
      disk2.expectSuperBlock (sb2)
    }

    "attach two new disks" in {
      val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val sys = new RichDiskSystem (scheduler)
      sys.attachAndPass (("a", disk1, big))

      val disk2 = new RichStubFile (scheduler)
      val disk3 = new RichStubFile (scheduler)
      sys.attachAndPass (("b", disk2, big), ("c", disk3, big))
      sys.expectDisks (2) ("a", "b", "c")
      val sb1 = mkSuperBlock (1, Set ("a"), big)
      disk1.expectSuperBlock (sb1)
      val sb2 = mkSuperBlock (2, Set ("a", "b", "c"), big)
      disk1.expectSuperBlock (sb2)
      disk2.expectSuperBlock (sb2)
      disk3.expectSuperBlock (sb2)
    }

    "pass through an attach failure and remain ready" in {
      val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val sys = new RichDiskSystem (scheduler)
      sys.attachAndPass (("a", disk1, big))

      val disk2 = new MockFile
      sys.attachAndFail (("b", disk2, big))
      sys.expectDisks (1) ("a")
    }

    "reject reattaching a disk" in {
      val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val sys = new RichDiskSystem (scheduler)
      sys.attachAndPass (("a", disk1, big))

      val disk2 = new MockFile
      sys.reattachAndFail [RecoveryCompletedException] (("b", disk2))
      val sb1 = mkSuperBlock (1, Set ("a"), big)
      disk1.expectSuperBlock (sb1)
    }

    "checkpoint" in {
      val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val sys = new RichDiskSystem (scheduler)
      sys.attachAndPass (("a", disk1, big))

      sys.checkpointAndPass()
      disk1.expectSuperBlock (mkSuperBlock (2, Set ("a"), big))
      disk1.expectSuperBlock (mkSuperBlock (1, Set ("a"), big))
    }

    "pass through a failed checkpoint and panic" in {
      val scheduler = StubScheduler.random()
      val disk1 = new MockFile
      val sys = new RichDiskSystem (scheduler)
      var cb = new CallbackCaptor [Unit]
      sys._attach (Seq (("a", disk1, big)), cb)
      disk1.expectFlush (SuperBlockBytes, 0, 33)
      scheduler.runTasks()
      assert (!cb.wasInvoked)
      disk1.completeLast()
      scheduler.runTasks()
      cb.passed
      sys.expectDisks (1) ("a")
      sys.assertReady()

      cb = new CallbackCaptor [Unit]
      sys.checkpoint (cb)
      disk1.expectFlush (0, 0, 33)
      scheduler.runTasks()
      assert (!cb.wasInvoked)
      disk1.failLast (new Exception)
      scheduler.runTasks()
      cb.failed
      sys.expectDisks (2) ("a")
      sys.assertPanicked()
    }}

  "When the DiskSystem is Attaching it should" - {

    "queue a second attach then complete it after the first attach" in {
      val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val sys = new RichDiskSystem (scheduler)
      sys.attachAndHold (("a", disk1, big))

      val disk2 = new RichStubFile (scheduler)
      sys.attachAndPass (("b", disk2, big))
      sys.expectDisks (2) ("a", "b")
      expectResult (2) (sys.disks.size)
      disk1.expectSuperBlock(mkSuperBlock (1, Set ("a"), big))
      disk1.expectSuperBlock(mkSuperBlock (2, Set ("a", "b"), big))
      disk2.expectSuperBlock(mkSuperBlock (2, Set ("a", "b"), big))
    }

    "reject reattaching a disk" in {
      val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val sys = new RichDiskSystem (scheduler)
      sys.attachAndHold (("a", disk1, big))

      val disk2 = new MockFile
      sys.reattachAndFail [RecoveryCompletedException] (("b", disk2))
      sys.expectDisks (1) ("a")
      disk1.expectSuperBlock (mkSuperBlock (1, Set ("a"), big))
    }

    "queue a checkpoint and complete it with the attach" in {
      val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val sys = new RichDiskSystem (scheduler)
      sys.attachAndHold (("a", disk1, big))

      sys.checkpointAndPass()
      sys.expectDisks (1) ("a")
      disk1.expectSuperBlock (mkSuperBlock (1, Set ("a"), big))
    }}

  "When the DiskSystem is Reattaching it should" - {

    "queue an attach then complete it after the reattach" in {
      val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      disk1.writeSuperBlock( mkSuperBlock (1, Set ("a"), big))
      val sys = new RichDiskSystem (scheduler)
      sys.reattachAndHold (("a", disk1))

      val disk2 = new RichStubFile (scheduler)
      sys.attachAndPass (("b", disk2, big))
      sys.expectDisks (2) ("a", "b")
      disk1.expectSuperBlock(mkSuperBlock (1, Set ("a"), big))
      disk1.expectSuperBlock(mkSuperBlock (2, Set ("a", "b"), big))
      disk2.expectSuperBlock(mkSuperBlock (2, Set ("a", "b"), big))
    }

    "reject reattaching a disk" in {
      val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      disk1.writeSuperBlock( mkSuperBlock (1, Set ("a"), big))
      val sys = new RichDiskSystem (scheduler)
      sys.reattachAndHold (("a", disk1))

      val disk2 = new MockFile
      sys.reattachAndFail [RecoveryCompletedException] (("b", disk2))
      sys.expectDisks (1) ("a")
      disk1.expectSuperBlock (mkSuperBlock (1, Set ("a"), big))
    }

    "queue a checkpoint and complete it after the reattach" in {
      val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      disk1.writeSuperBlock( mkSuperBlock (1, Set ("a"), big))
      val sys = new RichDiskSystem (scheduler)
      sys.reattachAndHold (("a", disk1))

      sys.checkpointAndPass()
      sys.expectDisks (2) ("a")
      disk1.expectSuperBlock (mkSuperBlock (1, Set ("a"), big))
      disk1.expectSuperBlock (mkSuperBlock (2, Set ("a"), big))
    }
  }

  "When the DiskSystem is Checkpointing it should" - {

    "queue an attach then complete it after the checkpoint" in {
      val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val sys = new RichDiskSystem (scheduler)
      sys.attachAndPass (("a", disk1, big))
      sys.checkpointAndHold()

      val disk2 = new RichStubFile (scheduler)
      sys.attachAndPass (("b", disk2, big))
      sys.expectDisks (3) ("a", "b")
      disk1.expectSuperBlock(mkSuperBlock (2, Set ("a"), big))
      disk1.expectSuperBlock(mkSuperBlock (3, Set ("a", "b"), big))
      disk2.expectSuperBlock(mkSuperBlock (3, Set ("a", "b"), big))
    }

    "reject reattaching a disk" in {
      val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val sys = new RichDiskSystem (scheduler)
      sys.attachAndPass (("a", disk1, big))
      sys.checkpointAndHold()

      val disk2 = new MockFile
      sys.reattachAndFail [RecoveryCompletedException] (("b", disk2))
      sys.expectDisks (2) ("a")
      disk1.expectSuperBlock (mkSuperBlock (1, Set ("a"), big))
      disk1.expectSuperBlock (mkSuperBlock (2, Set ("a"), big))
    }

    "queue a second checkpoint and complete it with the first checkpoint" in {
      val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile (scheduler)
      val sys = new RichDiskSystem (scheduler)
      sys.attachAndPass (("a", disk1, big))
      sys.checkpointAndHold()

      sys.checkpointAndPass()
      sys.expectDisks (2) ("a")
      disk1.expectSuperBlock (mkSuperBlock (1, Set ("a"), big))
      disk1.expectSuperBlock (mkSuperBlock (2, Set ("a"), big))
    }}}
