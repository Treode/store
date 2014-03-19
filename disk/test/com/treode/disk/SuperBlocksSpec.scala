package com.treode.disk

import java.nio.file.Paths

import com.treode.async.{AsyncTestTools, StubScheduler}
import com.treode.async.io.StubFile
import com.treode.buffer.PagedBuffer
import org.scalatest.FlatSpec

import AsyncTestTools._

class SuperBlocksSpec extends FlatSpec {

  val config = DisksConfig (0, 8, 1<<8, 10, 3, 1)
  val path = Paths.get ("a")

  private def setup() = {
    implicit val scheduler = StubScheduler.random()
    val file = new StubFile
    val geom = DiskGeometry (10, 4, 1<<20) (config)
    val free = IntSet()
    val boot0 = BootBlock (0, 0, 0, Set.empty)
    val superb0 = new SuperBlock (0, boot0, geom, false, free, 0, 0, 0, 0)
    val boot1 = BootBlock (0, 1, 0, Set.empty)
    val superb1 = new SuperBlock (0, boot1, geom, false, free, 0, 0, 0, 0)
    SuperBlock.write (superb0, file) (config) .pass
    SuperBlock.write (superb1, file) (config) .pass
    (scheduler, file, superb0, superb1)
  }

  "SuperBlocks.read" should "read what was written" in {
    implicit val (scheduler, file, superb0, superb1) = setup()
    val superbs = SuperBlocks.read (path, file) (config) .pass
    assertResult (Some (superb0)) (superbs.sb0)
    assertResult (Some (superb1)) (superbs.sb1)
  }

  it should "reject gen0 when it's hosed" in {
    implicit val (scheduler, file, superb0, superb1) = setup()
    val buf = PagedBuffer (12)
    buf.writeInt (0xC84404F5)
    file.flush (buf, 17) .pass
    val superbs = SuperBlocks.read (path, file) (config) .pass
    assertResult (None) (superbs.sb0)
    assertResult (Some (superb1)) (superbs.sb1)
  }

  it should "reject gen1 when it's hosed" in {
    implicit val (scheduler, file, superb0, superb1) = setup()
    val buf = PagedBuffer (12)
    buf.writeInt (0xC84404F5)
    file.flush (buf, config.superBlockBytes + 17) .pass
    val superbs = SuperBlocks.read (path, file) (config) .pass
    assertResult (Some (superb0)) (superbs.sb0)
    assertResult (None) (superbs.sb1)
  }}
