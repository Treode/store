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

package com.treode.twitter.app

import java.nio.file.Paths

import com.treode.cluster.{CellId, HostId}
import com.treode.store.Store
import com.twitter.app.App
import com.twitter.conversions.storage._
import com.twitter.util.StorageUnit

/** A Twitter [[$TwitterApp App]] to initialize the disk drives.
  *
  * Usage: Init [flags] path [paths]
  *
  *   - superBlockBits (default 14, 16KB): The size of the superblock in bits.
  *
  *   - segmentBits (default 30, 1GB): The size of a disk segment in bits.
  *
  *   - blockBits (default 13, 8KB): The size of a disk block in bits.
  *
  *   - diskBytes (default 1TB): The maximum size of the disk in bytes.
  *
  *   - cell: The ID of the cell this host will join.
  *
  *   - host: The ID of this host within the cell.
  *
  * The command initializes the repository to use the given path(s). The paths may name files or
  * raw disks. More paths can be added and some paths removed later while the system is running.
  *
  * There are no convenient tools in Java to determine geometry of raw disks (that is, it's block
  * size and number of blocks), so you must supply that information. All paths listed will be given
  * the same geometry. To run a repository with disks of different geometries, initialize the
  * system with disks that share the same geometry, and then add the other disks to the running
  * system later.
  *
  * The disk system needs to know the size of the superblocks to read them. Then it can find all
  * other information it requires to begin recovery. You must supply the same value for
  * `superBlockBits` to both initialization and recovery.
  *
  * The disk system allocates and cleans space in segments.
  *
  * All peers cooperating to serve the replicas and shards of a repository must use the same cell
  * ID. For users who are running multiple cells, this feature protects them from having peers of
  * different cells confusing each others. Each peer in the cell must have its own host ID, which
  * is then used in the cohort [[com.treode.store.Atlas Atlas]].
  *
  * The cell ID relates the peers, and the host ID relates this peer to the atlas. In other words,
  * these IDs determine which shards and replicas are stored on this machine's local disks. Once
  * you choose a cell and host ID, you cannot change them, ever. So, the system stores the IDs on
  * disk during initialization. You supply the IDs at that time, and the system finds them on
  * recovery.
  *
  * @define TwitterApp http://twitter.github.io/util/docs/#com.twitter.app.App
  */
class Init extends App {

  private val superBlockBits =
      flag [Int] ("superBlockBits",  14, "Size of the super block (log base 2)")

  private val segmentBits =
      flag [Int] ("segmentBits", 30, "Size of a disk segment (log base 2)")

  private val blockBits =
      flag [Int] ("blockBits", 13, "Size of a disk block (log base 2)")

  private val diskBytes =
      flag [StorageUnit] ("diskBytes", 1.terabyte, "Maximum size of disk (bytes)")

  private val cell = flag [CellId] ("cell", "Cell ID")

  private val host = flag [HostId] ("host", "Host ID")

  def main() {

    if (!cell.isDefined || !host.isDefined) {
      println ("-cell and -host are required.")
      return
    }

    if (args.length == 0) {
      println ("At least one path is required.")
      System.exit (1)
    }

    Store.init (
        host(),
        cell(),
        superBlockBits(),
        segmentBits(),
        blockBits(),
        diskBytes().inBytes,
        args map (Paths.get (_)): _*)
  }}
