package com.treode.disk

import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.StubScheduler
import com.treode.buffer.PagedBuffer
import org.scalatest.FlatSpec

import DiskTestTools._

class SuperBlockSpec extends FlatSpec {

  "SuperBlock.write" should "reject oversized superblocks" in {

    // Setup the config with a small space for the superblock.
    implicit val scheduler = StubScheduler.random()
    val config = TestDisksConfig (
        superBlockBits = 4,
        maximumRecordBytes = 1<<6,
        maximumPageBytes = 1<<6)
    val boot = BootBlock (0, 0, 0, Set.empty)
    val geom = TestDiskGeometry (blockBits=4) (config)
    val free = IntSet()
    val superb = new SuperBlock (0, boot, geom, false, free, 0)

    // Write something known to the file.
    val buf = PagedBuffer (12)
    for (i <- 0 until 1024)
      buf.writeInt (i)
    val file = new StubFile (1<<12)
    file.flush (buf, 0) .pass

    // Check that the write throws an exception.
    SuperBlock.write (superb, file) (config) .fail [SuperblockOverflowException]

    // Check that the file has not been overwritten.
    buf.clear()
    file.fill (buf, 0, 1024) .pass
    for (i <- 0 until 1024)
      assertResult (i) (buf.readInt())
  }}
