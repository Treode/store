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

package com.treode.disk

import java.nio.file.{Path, Paths}
import scala.util.Random

import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.buffer.PagedBuffer
import org.scalatest.FreeSpec

import DiskTestTools._

class SuperBlocksSpec extends FreeSpec {

  implicit val config = DiskTestConfig()
  val path = Paths.get ("a")
  val geom = DriveGeometry.test()

  private def superb (gen: Int, disk: Set [Path] = Set (path)) = {
    val free = IntSet()
    val boot = BootBlock (sysid, gen, 0, disk)
    new SuperBlock (0, boot, geom, false, free, 0)
  }

  private def superbs (
      gen0: Int,
      gen1: Int,
      path: Path = path,
      disk: Set [Path] = Set (path)
  ) = {
    val sb0 = if (gen0 < 0) None else Some (superb (gen0, disk))
    val sb1 = if (gen1 < 0) None else Some (superb (gen1, disk))
    new SuperBlocks (path, null, sb0, sb1)
  }

  private def chooseSuperBlock (reads: SuperBlocks*) =
    SuperBlocks.chooseSuperBlock (reads)

  private def verifyReattachment (reads: SuperBlocks*) (implicit config: DiskConfig) =
    SuperBlocks.verifyReattachment (reads) (config)

  private def setup() = {
    implicit val scheduler = StubScheduler.random()
    val file = StubFile (1<<20, geom.blockBits)
    val superb0 = superb (0)
    val superb1 = superb (1)
    SuperBlock.write (superb0, file) (config) .expectPass()
    SuperBlock.write (superb1, file) (config) .expectPass()
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

    "accept a singleton list of superblocks that matches the boot block's disk" in {
      verifyReattachment (superbs (0, 1))
    }

    "accept a list of superblocks that matches the boot block's disk" in {
      val pathb = Paths.get ("b")
      val pathc = Paths.get ("c")
      val disk = Set (path, pathb, pathc)
      verifyReattachment (
          superbs (0, 1, path, disk),
          superbs (0, 1, pathb, disk),
          superbs (0, 1, pathc, disk))
    }

    "require some superblocks" in {
      intercept [NoSuperBlocksException] {
        verifyReattachment()
      }}

    "require the list of superblocks contain all of the disk in the boot block" in {
      val pathb = Paths.get ("b")
      val pathc = Paths.get ("c")
      val disk = Set (path, pathb, pathc)
      intercept [MissingDisksException] {
        verifyReattachment (superbs (0, 1, path, disk), superbs (0, 1, pathc, disk))
      }
    }

    "require the list of superblocks contain only the disk in the boot block" in {
      val pathb = Paths.get ("b")
      val pathc = Paths.get ("c")
      val disk = Set (path, pathc)
      intercept [ExtraDisksException] {
        verifyReattachment (
            superbs (0, 1, path, disk),
            superbs (0, 1, pathb, disk),
            superbs (0, 1, pathc, disk))
      }}}

  "SuperBlocks.read should" - {

    "read what was written" in {
      implicit val (scheduler, file, superb0, superb1) = setup()
      val superbs = SuperBlocks.read (path, file) (config) .expectPass()
      assertResult (Some (superb0)) (superbs.sb0)
      assertResult (Some (superb1)) (superbs.sb1)
    }

    "reject gen0 when it's hosed" in {
      implicit val (scheduler, file, _, superb1) = setup()
      val buf = PagedBuffer (12)
      while (buf.readableBytes < geom.blockBytes)
        buf.writeInt (Random.nextInt)
      file.flush (buf, 0) .expectPass()
      val superbs = SuperBlocks.read (path, file) (config) .expectPass()
      assertResult (None) (superbs.sb0)
      assertResult (Some (superb1)) (superbs.sb1)
    }

    "reject gen1 when it's hosed" in {
      implicit val (scheduler, file, superb0, _) = setup()
      val buf = PagedBuffer (12)
      while (buf.readableBytes < geom.blockBytes)
        buf.writeInt (Random.nextInt)
      file.flush (buf, config.superBlockBytes) .expectPass()
      val superbs = SuperBlocks.read (path, file) (config) .expectPass()
      assertResult (Some (superb0)) (superbs.sb0)
      assertResult (None) (superbs.sb1)
    }}}
