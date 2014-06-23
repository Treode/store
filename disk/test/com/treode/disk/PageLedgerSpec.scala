package com.treode.disk

import scala.util.Random

import com.treode.async.stubs.StubScheduler
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.implicits._
import com.treode.buffer.PagedBuffer
import org.scalatest.FlatSpec

import DiskTestTools._

class PageLedgerSpec extends FlatSpec {

  "PageLedger.write" should "reject oversized ledgers" in {

    implicit val random = new Random
    implicit val scheduler = StubScheduler.random (random)
    implicit val config = DiskTestConfig()
    val geom = DriveGeometry.test()

    // Make a large ledger.
    val ledger = new PageLedger
    for (_ <- 0 until 256)
      ledger.add (random.nextInt, random.nextLong, random.nextGroup, 128)

    // Write something known to the file.
    val buf = PagedBuffer (12)
    for (i <- 0 until 1024)
      buf.writeInt (i)
    val file = StubFile (1<<20, 6)
    file.flush (buf, 0) .pass

    // Check that the write throws an exception.
    PageLedger.write (ledger, file, geom, 0, 256) .fail [PageLedgerOverflowException]

    // Check that the file has not been overwritten.
    buf.clear()
    file.fill (buf, 0, 1024) .pass
    for (i <- 0 until 1024)
      assertResult (i) (buf.readInt())
  }}
