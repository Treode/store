package com.treode.disk

import java.nio.file.{Path, Paths}

import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.buffer.PagedBuffer
import org.scalatest.FreeSpec

import DiskTestTools._

class SuperBlocksSpec extends FreeSpec {

  implicit val config = DiskTestConfig()
  val path = Paths.get ("a")
  val geom = DiskGeometry.test()

  private def superb (gen: Int, disks: Set [Path] = Set (path)) = {
    val free = IntSet()
    val boot = BootBlock (0, gen, 0, disks)
    new SuperBlock (0, boot, geom, false, free, 0)
  }

  private def superbs (
      gen0: Int,
      gen1: Int,
      path: Path = path,
      disks: Set [Path] = Set (path)
  ) = {
    val sb0 = if (gen0 < 0) None else Some (superb (gen0, disks))
    val sb1 = if (gen1 < 0) None else Some (superb (gen1, disks))
    new SuperBlocks (path, null, sb0, sb1)
  }

  private def chooseSuperBlock (reads: SuperBlocks*) =
    SuperBlocks.chooseSuperBlock (reads)

  private def verifyReattachment (reads: SuperBlocks*) (implicit config: DiskConfig) =
    SuperBlocks.verifyReattachment (reads) (config)

  private def setup() = {
    implicit val scheduler = StubScheduler.random()
    val file = StubFile()
    val superb0 = superb (0)
    val superb1 = superb (1)
    SuperBlock.write (superb0, file) (config) .pass
    SuperBlock.write (superb1, file) (config) .pass
    (scheduler, file, superb0, superb1)
  }

  "When chooseSuperBlock is given" - {

    "no reads, it should raise an exception" in {
      intercept [NoSuperBlocksException] {
        chooseSuperBlock()
      }}

    "one read with" - {

      "no superblocks, it should raise an exception" in {
        intercept [NoSuperBlocksException] {
          chooseSuperBlock (superbs (-1, -1))
        }}

      "only a gen0 superblock, it should choose that" in {
        assertResult (true) {
          chooseSuperBlock (superbs (0, -1))
        }}

      "only a gen1 superblock, it should choose that" in {
        assertResult (false) {
          chooseSuperBlock (superbs (-1, 1))
        }}

      "a newer gen0 superblock, it should choose that" in {
        assertResult (true) {
          chooseSuperBlock (superbs (2, 1))
        }}

      "a newer gen1 superblock, it should choose that" in {
        assertResult (false) {
          chooseSuperBlock (superbs (0, 1))
        }}}

    "two reads" - {

      "the first with no superblocks. and the second with" - {

        "no superblocks, it should raise an exception" in {
          intercept [NoSuperBlocksException] {
            chooseSuperBlock (superbs (-1, -1), superbs (-1, -1))
          }}

        "only a gen0 superblock, it should raise an exception" in {
          intercept [InconsistentSuperBlocksException] {
            chooseSuperBlock (superbs (-1, -1), superbs (0, -1))
          }}

        "only a gen1 superblock, it should raise an exception" in {
          intercept [InconsistentSuperBlocksException] {
            chooseSuperBlock (superbs (-1, -1), superbs (-1, 1))
          }}

        "a newer gen0 superblock, it should raise an exception" in {
          intercept [InconsistentSuperBlocksException] {
            chooseSuperBlock (superbs (-1, -1), superbs (2, 1))
          }}

        "a newer gen1 superblock, it should raise an exception" in {
          intercept [InconsistentSuperBlocksException] {
            chooseSuperBlock (superbs (-1, -1), superbs (0, 1))
          }}
      }

      "the first with only a gen0 superblock, and the second with" - {

        "no superblocks, it should raise an exception" in {
          intercept [InconsistentSuperBlocksException] {
            chooseSuperBlock (superbs (0, -1), superbs (-1, -1))
          }}

        "only a gen0 superblock that matches, it should choose that" in {
          assertResult (true) {
            chooseSuperBlock (superbs (0, -1), superbs (0, -1))
          }}

        "only a gen0 superblock that's newer, it should raise an exception" in {
          intercept [InconsistentSuperBlocksException] {
            chooseSuperBlock (superbs (0, -1), superbs (2, -1))
          }}

        "only a gen1 superblock, it should raise an exception" in {
          intercept [InconsistentSuperBlocksException] {
            chooseSuperBlock (superbs (0, -1), superbs (-1, 1))
          }}

        "a newer gen0 superblock, it should raise an exception" in {
          intercept [InconsistentSuperBlocksException] {
            chooseSuperBlock (superbs (0, -1), superbs (2, 1))
          }}

        "a newer gen1 superblock, it should choose the older consistent superblock" in {
          assertResult (true) {
            chooseSuperBlock (superbs (0, -1), superbs (0, 1))
          }}}

      "the first with only a gen1 superblock, and the second with" - {

        "no superblocks, it should raise an exception" in {
          intercept [InconsistentSuperBlocksException] {
            chooseSuperBlock (superbs (-1, 1), superbs (-1, -1))
          }}

        "only a gen0 superblock, it should raise an exception" in {
          intercept [InconsistentSuperBlocksException] {
            chooseSuperBlock (superbs (-1, 1), superbs (0, -1))
          }}

        "only a gen1 superblock that matches, it should choose that" in {
          assertResult (false) {
            chooseSuperBlock (superbs (-1, 1), superbs (-1, 1))
          }}

        "only a gen1 superblock that's newer, it should raise an exception" in {
          intercept [InconsistentSuperBlocksException] {
            chooseSuperBlock (superbs (-1, 1), superbs (-1, 3))
          }}

        "a newer gen0 superblock, it should choose the older consistent superblock" in {
          assertResult (false) {
            chooseSuperBlock (superbs (-1, 1), superbs (2, 1))
          }}

        "a newer gen1 superblock, it should choose the older consistent superblock" in {
          intercept [InconsistentSuperBlocksException] {
            chooseSuperBlock (superbs (-1, 1), superbs (0, 3))
          }}}

      "the first with a newer gen0 superblock, and the second with" - {

        "no superblocks, it should raise an exception" in {
          intercept [InconsistentSuperBlocksException] {
            chooseSuperBlock (superbs (2, 1), superbs (-1, -1))
          }}

        "only a gen0 superblock that matches, it should choose that" in {
          assertResult (true) {
            chooseSuperBlock (superbs (2, 1), superbs (2, -1))
          }}

        "only a gen0 superblock that's older, it should raise an exception" in {
          intercept [InconsistentSuperBlocksException] {
            chooseSuperBlock (superbs (2, 1), superbs (0, -1))
          }}

        "only a gen1 superblock that matches, it should choose that" in {
          assertResult (false) {
            chooseSuperBlock (superbs (2, 1), superbs (-1, 1))
          }}

        "only a gen1 superblock that's older, it should raise an exception" in {
          intercept [InconsistentSuperBlocksException] {
            chooseSuperBlock (superbs (4, 3), superbs (-1, 1))
          }}

        "matching gen0 and gen1, it should choose the newer consistent superblock" in {
          assertResult (true) {
            chooseSuperBlock (superbs (2, 1), superbs (2, 1))
          }}

        "a newer gen1 superblock, it should choose the older consistent superblock" in {
          assertResult (true) {
            chooseSuperBlock (superbs (2, 1), superbs (2, 3))
          }}}

       "the first with a newer gen1 superblock, and the second with" - {

        "no superblocks, it should raise an exception" in {
          intercept [InconsistentSuperBlocksException] {
            chooseSuperBlock (superbs (0, 1), superbs (-1, -1))
          }}

        "only a gen0 superblock that matches, it should choose that" in {
          assertResult (true) {
            chooseSuperBlock (superbs (0, 1), superbs (0, -1))
          }}

        "only a gen0 superblock that's older, it should raise an exception" in {
          intercept [InconsistentSuperBlocksException] {
            chooseSuperBlock (superbs (2, 3), superbs (0, -1))
          }}

        "only a gen1 superblock that matches, it should choose that" in {
          assertResult (false) {
            chooseSuperBlock (superbs (0, 1), superbs (-1, 1))
          }}

        "only a gen1 superblock that's older, it should raise and exception" in {
          intercept [InconsistentSuperBlocksException] {
            chooseSuperBlock (superbs (2, 3), superbs (-1, 1))
          }}

        "matching gen0 and gen1, it should choose the newer consistent superblock" in {
          assertResult (false) {
            chooseSuperBlock (superbs (0, 1), superbs (0, 1))
          }}

        "a newer gen1 superblock, it should choose the older consistent superblock" in {
          assertResult (true) {
            chooseSuperBlock (superbs (0, 1), superbs (0, 3))
          }}}}}

  "SuperBlocks.verifyReattachment should" - {

    "accept a singleton list of superblocks that matches the boot block's disks" in {
      verifyReattachment (superbs (0, 1))
    }

    "accept a list of superblocks that matches the boot block's disks" in {
      val pathb = Paths.get ("b")
      val pathc = Paths.get ("c")
      val disks = Set (path, pathb, pathc)
      verifyReattachment (
          superbs (0, 1, path, disks),
          superbs (0, 1, pathb, disks),
          superbs (0, 1, pathc, disks))
    }

    "require some superblocks" in {
      intercept [NoSuperBlocksException] {
        verifyReattachment()
      }}

    "require the config's cell match the boot block's cell" in {
      val config2 = DiskTestConfig (cell = 1)
      intercept [CellMismatchException] {
        verifyReattachment (superbs (0, 1)) (config2)
      }}

    "require the list of superblocks contain all of the disks in the boot block" in {
      val pathb = Paths.get ("b")
      val pathc = Paths.get ("c")
      val disks = Set (path, pathb, pathc)
      intercept [MissingDisksException] {
        verifyReattachment (superbs (0, 1, path, disks), superbs (0, 1, pathc, disks))
      }
    }

    "require the list of superblocks contain only the disks in the boot block" in {
      val pathb = Paths.get ("b")
      val pathc = Paths.get ("c")
      val disks = Set (path, pathc)
      intercept [ExtraDisksException] {
        verifyReattachment (
            superbs (0, 1, path, disks),
            superbs (0, 1, pathb, disks),
            superbs (0, 1, pathc, disks))
      }}}

  "SuperBlocks.read should" - {

    "read what was written" in {
      implicit val (scheduler, file, superb0, superb1) = setup()
      val superbs = SuperBlocks.read (path, file) (config) .pass
      assertResult (Some (superb0)) (superbs.sb0)
      assertResult (Some (superb1)) (superbs.sb1)
    }

    "reject gen0 when it's hosed" in {
      implicit val (scheduler, file, superb0, superb1) = setup()
      val buf = PagedBuffer (12)
      buf.writeInt (0xC84404F5)
      file.flush (buf, 17) .pass
      val superbs = SuperBlocks.read (path, file) (config) .pass
      assertResult (None) (superbs.sb0)
      assertResult (Some (superb1)) (superbs.sb1)
    }

    "reject gen1 when it's hosed" in {
      implicit val (scheduler, file, superb0, superb1) = setup()
      val buf = PagedBuffer (12)
      buf.writeInt (0xC84404F5)
      file.flush (buf, config.superBlockBytes + 17) .pass
      val superbs = SuperBlocks.read (path, file) (config) .pass
      assertResult (Some (superb0)) (superbs.sb0)
      assertResult (None) (superbs.sb1)
    }}}
