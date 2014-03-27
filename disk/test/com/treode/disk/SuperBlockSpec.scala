package com.treode.disk

import com.treode.async.{AsyncTestTools, StubScheduler}
import com.treode.async.io.StubFile
import com.treode.buffer.PagedBuffer
import org.scalatest.FlatSpec

import AsyncTestTools._

class SuperBlockSpec extends FlatSpec {

  "SuperBlock.write" should "reject oversized superblocks" in {

    // Setup the config with a small space for the superblock.
    implicit val scheduler = StubScheduler.random()
    val config = DisksConfig (0, 4, 1<<8, 10, 3, 1)
    val boot = BootBlock (0, 0, 0, Set.empty)
    val geom = DiskGeometry (8, 4, 1<<12) (config)
    val free = IntSet()
    val superb = new SuperBlock (0, boot, geom, false, free, 0, 0)

    // Write something known to the file.
    val buf = PagedBuffer (12)
    for (i <- 0 until 1024)
      buf.writeInt (i)
    val file = new StubFile
    file.flush (buf, 0) .pass

    // Check that the write throws an exception.
    SuperBlock.write (superb, file) (config) .fail [SuperblockOverflowException]

    // Check that the file has not been overwritten.
    buf.clear()
    file.fill (buf, 0, 1024) .pass
    for (i <- 0 until 1024)
      assertResult (i) (buf.readInt())
  }}
