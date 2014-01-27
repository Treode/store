package com.treode.disk

import java.nio.file.Paths
import scala.language.implicitConversions

import com.treode.async._
import com.treode.async.io.{File, MockFile, StubFile}
import com.treode.buffer.PagedBuffer
import com.treode.pickle.{pickle, unpickle}
import org.scalatest.FreeSpec

class DisksKitSpec extends FreeSpec {

  val config = DiskDriveConfig (16, 12, 1L<<20)

  private def mkSuperBlock (file: File, id: Int, gen: Int, disks: Set [String],
      config: DiskDriveConfig, scheduler: StubScheduler) = {
    val alloc = new SegmentAllocator (config)
    alloc.init()
    val log = new LogWriter (file, alloc, scheduler, null)
    log.init (Callback.ignore)
    val pages = new PageWriter (id, file, config, alloc, scheduler, null)
    pages.init()
    val roots = RootRegistry.Meta.empty
    scheduler.runTasks()
    val boot = BootBlock (
        gen,
        disks map (Paths.get (_)),
        roots)
    SuperBlock (
        id,
        boot,
        config,
        alloc.checkpoint (gen),
        log.checkpoint (gen),
        pages.checkpoint (gen))
  }

  private class RichStubFile (implicit scheduler: StubScheduler) extends StubFile {

    def readSuperBlock (gen: Int): SuperBlock = {
      val buffer = PagedBuffer (12)
      val pos = if ((gen & 1) == 0) 0 else SuperBlockBytes
      val cb = new CallbackCaptor [Unit]
      fill (buffer, pos, 1, cb)
      scheduler.runTasks()
      cb.passed
      unpickle (SuperBlock.pickler, buffer)
    }

    def expectSuperBlock (id: Int, gen: Int, disks: Set [String], config: DiskDriveConfig) {
      val _disks = disks .map (Paths.get (_)) .toSet
      val block = readSuperBlock (gen)
      expectResult (id) (block.id)
      expectResult (gen) (block.boot.gen)
      expectResult (_disks) (block.boot.disks)
      expectResult (config) (block.config)
    }

    def writeSuperBlock (id: Int, gen: Int, disks: Set [String], config: DiskDriveConfig) {
      val block = mkSuperBlock (this, id, gen, disks, config, scheduler)
      val buffer = PagedBuffer (12)
      pickle (SuperBlock.pickler, block, buffer)
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

  "When the DiskSystem is Opening it should" - {

    "attach a new disk" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile
      val kit = new RichDisksKit
      kit.attachAndPass (("a", disk1, config))
      kit.expectDisks (1) ((0, "a"))
      disk1.expectSuperBlock (0, 1, Set ("a"), config)
    }

    "attach two new disks" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile
      val disk2 = new RichStubFile
      val kit = new RichDisksKit
      kit.attachAndPass (("a", disk1, config), ("b", disk2, config))
      kit.expectDisks (1) ((0, "a"), (1, "b"))
      disk1.expectSuperBlock (0, 1, Set ("a", "b"), config)
      disk2.expectSuperBlock (1, 1, Set ("a", "b"), config)
    }

    "pass through an attach failure and remain opening" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new MockFile
      val kit = new RichDisksKit
      kit.attachAndFail (("a", disk1, config))
      kit.expectDisks (1) ()
    }

    "reattach a disk with only the first superblock" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile
      disk1.writeSuperBlock (0, 2, Set ("a"), config)
      disk1.destroySuperBlock (1)
      val kit = new RichDisksKit
      kit.reattachAndPass (("a", disk1))
      kit.expectDisks (2) ((0, "a"))
    }

    "reattach a disk with only the second superblock" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile
      disk1.destroySuperBlock (0)
      disk1.writeSuperBlock (0, 1, Set ("a"), config)
      val kit = new RichDisksKit
      kit.reattachAndPass (("a", disk1))
      kit.expectDisks (1) ((0, "a"))
    }

    "reattach a disk with the first superblock as newest" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile
      disk1.writeSuperBlock (0, 4, Set ("a"), config)
      disk1.writeSuperBlock (0, 3, Set ("a"), config)
      val kit = new RichDisksKit
      kit.reattachAndPass (("a", disk1))
      kit.expectDisks (4) ((0, "a"))
    }

    "reattach a disk with the second superblock as newest" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile
      disk1.writeSuperBlock (0, 2, Set ("a"), config)
      disk1.writeSuperBlock (0, 3, Set ("a"), config)
      val kit = new RichDisksKit
      kit.reattachAndPass (("a", disk1))
      kit.expectDisks (3) ((0, "a"))
    }

    "reattach two disks with only the first superblock" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile
      disk1.writeSuperBlock (0, 2, Set ("a", "b"), config)
      disk1.destroySuperBlock (1)
      val disk2 = new RichStubFile
      disk2.writeSuperBlock (1, 2, Set ("a", "b"), config)
      disk2.destroySuperBlock (1)
      val kit = new RichDisksKit
      kit.reattachAndPass (("a", disk1), ("b", disk2))
      kit.expectDisks (2) ((0, "a"), (1, "b"))
    }

    "reattach two disks with only the second superblock" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile
      disk1.destroySuperBlock (0)
      disk1.writeSuperBlock (0, 1, Set ("a", "b"), config)
      val disk2 = new RichStubFile
      disk2.destroySuperBlock (0)
      disk2.writeSuperBlock (1, 1, Set ("a", "b"), config)
      val kit = new RichDisksKit
      kit.reattachAndPass (("a", disk1), ("b", disk2))
      kit.expectDisks (1) ((0, "a"), (1, "b"))
    }

    "reattach two disks with first superblock as newest" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile
      disk1.writeSuperBlock (0, 4, Set ("a", "b"), config)
      disk1.writeSuperBlock (0, 3, Set ("a", "b"), config)
      val disk2 = new RichStubFile
      disk2.writeSuperBlock (1, 4, Set ("a", "b"), config)
      disk2.writeSuperBlock (1, 3, Set ("a", "b"), config)
      val kit = new RichDisksKit
      kit.reattachAndPass (("a", disk1), ("b", disk2))
      kit.expectDisks (4) ((0, "a"), (1, "b"))
    }

    "reattach two disks with second superblock as newest" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile
      disk1.writeSuperBlock (0, 2, Set ("a", "b"), config)
      disk1.writeSuperBlock (0, 3, Set ("a", "b"), config)
      val disk2 = new RichStubFile
      disk2.writeSuperBlock (1, 2, Set ("a", "b"), config)
      disk2.writeSuperBlock (1, 3, Set ("a", "b"), config)
      val kit = new RichDisksKit
      kit.reattachAndPass (("a", disk1), ("b", disk2))
      kit.expectDisks (3) ((0, "a"), (1, "b"))
    }

    "reattach two disks with only first superblock in agreement" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile
      disk1.writeSuperBlock (0, 2, Set ("a", "b"), config)
      disk1.writeSuperBlock (0, 3, Set ("a", "b"), config)
      val disk2 = new RichStubFile
      disk2.writeSuperBlock (1, 2, Set ("a", "b"), config)
      disk2.destroySuperBlock (3)
      val kit = new RichDisksKit
      kit.reattachAndPass (("a", disk1), ("b", disk2))
      kit.expectDisks (2) ((0, "a"), (1, "b"))
    }

    "reattach two disks with only second superblock in agreement" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile
      disk1.writeSuperBlock (0, 4, Set ("a", "b"), config)
      disk1.writeSuperBlock (0, 3, Set ("a", "b"), config)
      val disk2 = new RichStubFile
      disk2.destroySuperBlock (4)
      disk2.writeSuperBlock (1, 3, Set ("a", "b"), config)
      val kit = new RichDisksKit
      kit.reattachAndPass (("a", disk1), ("b", disk2))
      kit.expectDisks (3) ((0, "a"), (1, "b"))
    }

    "panic on reattaching two disks with no superblocks in agreement" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile
      disk1.writeSuperBlock (0, 2, Set ("a", "b"), config)
      disk1.writeSuperBlock (0, 1, Set ("a", "b"), config)
      val disk2 = new RichStubFile
      disk2.writeSuperBlock (1, 4, Set ("a", "b"), config)
      disk2.writeSuperBlock (1, 3, Set ("a", "b"), config)
      val kit = new RichDisksKit
      kit.reattachAndFail [MultiException] (("a", disk1), ("b", disk2))
    }

    "panic on reattaching two disks with no superblocks fully present" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile
      disk1.writeSuperBlock (0, 4, Set ("a", "b"), config)
      disk1.destroySuperBlock (3)
      val disk2 = new RichStubFile
      disk2.destroySuperBlock (4)
      disk2.writeSuperBlock (1, 3, Set ("a", "b"), config)
      val kit = new RichDisksKit
      kit.reattachAndFail (("a", disk1), ("b", disk2))
    }

    "queue a checkpoint and complete it with attach" in {
      implicit val scheduler = StubScheduler.random()
      val kit = new RichDisksKit
      val cb = kit.checkpointAndQueue()
      kit.assertOpening()
      val disk1 = new RichStubFile
      kit.attachAndPass (("a", disk1, config))
      cb.passed
      disk1.expectSuperBlock (0, 1, Set ("a"), config)
    }

    "queue a checkpoint and complete it after reattach" in {
      implicit val scheduler = StubScheduler.random()
      val kit = new RichDisksKit
      val cb = kit.checkpointAndQueue()
      kit.assertOpening()
      val disk1 = new RichStubFile
      disk1.writeSuperBlock (0, 1, Set ("a"), config)
      disk1.destroySuperBlock (0)
      kit.reattachAndPass (("a", disk1))
      cb.passed
      disk1.expectSuperBlock (0, 2, Set ("a"), config)
      disk1.expectSuperBlock (0, 1, Set ("a"), config)
    }}

  "When the DiskSystem is Ready it should" - {

    "attach a new disk" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile
      val kit = new RichDisksKit
      kit.attachAndPass (("a", disk1, config))

      val disk2 = new RichStubFile
      kit.attachAndPass (("b", disk2, config))
      kit.expectDisks (2) ((0, "a"), (1, "b"))
      disk1.expectSuperBlock (0, 1, Set ("a"), config)
      disk1.expectSuperBlock (0, 2, Set ("a", "b"), config)
      disk2.expectSuperBlock (1, 2, Set ("a", "b"), config)
    }

    "attach two new disks" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile
      val kit = new RichDisksKit
      kit.attachAndPass (("a", disk1, config))

      val disk2 = new RichStubFile
      val disk3 = new RichStubFile
      kit.attachAndPass (("b", disk2, config), ("c", disk3, config))
      kit.expectDisks (2) ((0, "a"), (1, "b"), (2, "c"))
      disk1.expectSuperBlock (0, 1, Set ("a"), config)
      disk1.expectSuperBlock (0, 2, Set ("a", "b", "c"), config)
      disk2.expectSuperBlock (1, 2, Set ("a", "b", "c"), config)
      disk3.expectSuperBlock (2, 2, Set ("a", "b", "c"), config)
    }

    "pass through an attach failure and remain ready" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile
      val kit = new RichDisksKit
      kit.attachAndPass (("a", disk1, config))

      val disk2 = new MockFile
      kit.attachAndFail (("b", disk2, config))
      kit.expectDisks (1) ((0, "a"))
    }

    "reject reattaching a disk" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile
      val kit = new RichDisksKit
      kit.attachAndPass (("a", disk1, config))

      val disk2 = new MockFile
      kit.reattachAndFail [RecoveryCompletedException] (("b", disk2))
      disk1.expectSuperBlock (0, 1, Set ("a"), config)
    }

    "checkpoint" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile
      val kit = new RichDisksKit
      kit.attachAndPass (("a", disk1, config))

      kit.checkpointAndPass()
      disk1.expectSuperBlock (0, 2, Set ("a"), config)
      disk1.expectSuperBlock (0, 1, Set ("a"), config)
    }

    "pass through a failed checkpoint and panic" in { pending
      implicit val scheduler = StubScheduler.random()
      val disk1 = new MockFile
      val kit = new RichDisksKit
      var cb = kit.attachAndHold (("a", disk1, config))
      disk1.expectFlush (DiskLeadBytes, 0, 5)
      scheduler.runTasks()
      disk1.completeLast()
      disk1.expectFlush (SuperBlockBytes, 0, 47)
      scheduler.runTasks()
      assert (!cb.wasInvoked)
      disk1.completeLast()
      scheduler.runTasks()
      cb.passed
      kit.expectDisks (1) ((0, "a"))
      kit.assertReady()

      cb = new CallbackCaptor [Unit]
      kit.checkpoint (cb)
      disk1.expectFlush (0, 0, 0)
      scheduler.runTasks()
      disk1.expectFlush (0, 0, 47)
      scheduler.runTasks()
      disk1.completeLast()
      scheduler.runTasks()
      assert (!cb.wasInvoked)
      disk1.failLast (new Exception)
      scheduler.runTasks()
      cb.failed
      kit.expectDisks (2) ((0, "a"))
      kit.assertPanicked()
    }}

  "When the DiskSystem is Attaching it should" - {

    "queue a second attach then complete it after the first attach" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile
      val kit = new RichDisksKit
      kit.attachAndHold (("a", disk1, config))

      val disk2 = new RichStubFile
      kit.attachAndPass (("b", disk2, config))
      kit.expectDisks (2) ((0, "a"), (1, "b"))
      expectResult (2) (kit.disks.size)
      disk1.expectSuperBlock (0, 1, Set ("a"), config)
      disk1.expectSuperBlock (0, 2, Set ("a", "b"), config)
      disk2.expectSuperBlock (1, 2, Set ("a", "b"), config)
    }

    "reject reattaching a disk" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile
      val kit = new RichDisksKit
      kit.attachAndHold (("a", disk1, config))

      val disk2 = new MockFile
      kit.reattachAndFail [RecoveryCompletedException] (("b", disk2))
      kit.expectDisks (1) ((0, "a"))
      disk1.expectSuperBlock (0, 1, Set ("a"), config)
    }

    "queue a checkpoint and complete it with the attach" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile
      val kit = new RichDisksKit
      kit.attachAndHold (("a", disk1, config))

      kit.checkpointAndPass()
      kit.expectDisks (1) ((0, "a"))
      disk1.expectSuperBlock (0, 1, Set ("a"), config)
    }}

  "When the DiskSystem is Reattaching it should" - {

    "queue an attach then complete it after the reattach" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile
      disk1.writeSuperBlock (0, 1, Set ("a"), config)
      val kit = new RichDisksKit
      kit.reattachAndHold (("a", disk1))

      val disk2 = new RichStubFile
      kit.attachAndPass (("b", disk2, config))
      kit.expectDisks (2) ((0, "a"), (1, "b"))
      disk1.expectSuperBlock (0, 1, Set ("a"), config)
      disk1.expectSuperBlock (0, 2, Set ("a", "b"), config)
      disk2.expectSuperBlock (1, 2, Set ("a", "b"), config)
    }

    "reject reattaching a disk" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile
      disk1.writeSuperBlock (0, 1, Set ("a"), config)
      val kit = new RichDisksKit
      kit.reattachAndHold (("a", disk1))

      val disk2 = new MockFile
      kit.reattachAndFail [RecoveryCompletedException] (("b", disk2))
      kit.expectDisks (1) ((0, "a"))
      disk1.expectSuperBlock (0, 1, Set ("a"), config)
    }

    "queue a checkpoint and complete it after the reattach" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile
      disk1.writeSuperBlock (0, 1, Set ("a"), config)
      val kit = new RichDisksKit
      kit.reattachAndHold (("a", disk1))

      kit.checkpointAndPass()
      kit.expectDisks (2) ((0, "a"))
      disk1.expectSuperBlock (0, 1, Set ("a"), config)
      disk1.expectSuperBlock (0, 2, Set ("a"), config)
    }}

  "When the DiskSystem is Checkpointing it should" - {

    "queue an attach then complete it after the checkpoint" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile
      val kit = new RichDisksKit
      kit.attachAndPass (("a", disk1, config))
      kit.checkpointAndHold()

      val disk2 = new RichStubFile
      kit.attachAndPass (("b", disk2, config))
      kit.expectDisks (3) ((0, "a"), (1, "b"))
      disk1.expectSuperBlock (0, 2, Set ("a"), config)
      disk1.expectSuperBlock (0, 3, Set ("a", "b"), config)
      disk2.expectSuperBlock (1, 3, Set ("a", "b"), config)
    }

    "reject reattaching a disk" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile
      val kit = new RichDisksKit
      kit.attachAndPass (("a", disk1, config))
      kit.checkpointAndHold()

      val disk2 = new MockFile
      kit.reattachAndFail [RecoveryCompletedException] (("b", disk2))
      kit.expectDisks (2) ((0, "a"))
      disk1.expectSuperBlock (0, 1, Set ("a"), config)
      disk1.expectSuperBlock (0, 2, Set ("a"), config)
    }

    "queue a second checkpoint and complete it with the first checkpoint" in {
      implicit val scheduler = StubScheduler.random()
      val disk1 = new RichStubFile
      val kit = new RichDisksKit
      kit.attachAndPass (("a", disk1, config))
      kit.checkpointAndHold()

      kit.checkpointAndPass()
      kit.expectDisks (2) ((0, "a"))
      disk1.expectSuperBlock (0, 1, Set ("a"), config)
      disk1.expectSuperBlock (0, 2, Set ("a"), config)
    }}}
