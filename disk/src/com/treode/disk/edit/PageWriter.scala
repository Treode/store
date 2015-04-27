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

package com.treode.disk.edit

import java.nio.file.Path
import java.util.ArrayList
import scala.collection.JavaConversions._
import scala.collection.mutable.UnrolledBuffer
import scala.util.{Failure, Success}

import com.treode.async.{Async, Callback, Scheduler}, Async.{guard, supply}, Callback.ignore
import com.treode.async.implicits._
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer
import com.treode.disk.{DriveGeometry, PickledPage, Position, SegmentBounds}

/** Receives items from a PageDispatcher and writes them to a file. */
private class PageWriter (
  id: Int,
  path: Path,
  file: File,
  geom: DriveGeometry,
  dispatcher: PageDispatcher,
  detacher: Detacher,
  alloc: SegmentAllocator
) (implicit
  scheduler: Scheduler
) {

  import geom.{blockBits, blockAlignUp}

  // The current page segment.
  private var seg = SegmentBounds (-1, 0, 0)

  // The position of the latest batch that was written.
  private var pos = 0L

  // Something awaiting a draing to complete.
  private var drain = Option.empty [Callback [Unit]]

  private var ledger: SegmentLedger = null

  private def run [A] (task: Async [A]) {
    task run {
      case Success (_) => ()
      case Failure (t) => throw t
    }}

  private def register(): Unit =
    run {
      for {
        (_, pages) <- dispatcher.receive()
        enroll <- flush (pages)
      } yield {
        if (enroll)
          register()
      }}

  private def flush (pages: UnrolledBuffer [PickledPage]): Async [Boolean] =
    guard {

      if (!detacher.startFlush()) {
        dispatcher.send (pages)
        supply (false)
      } else {

        // Compute how many bytes we can write without overflowing the segment.
        val bytes = seg.limit - pos

        // Fit as many page as possible into the remainder of this segment; track which fit
        // (callbacks) and which to do not fit (putbacks).
        val buf = PagedBuffer (blockBits)
        val tally = new PageTally
        val callbacks = new ArrayList [Callback [Unit]] (pages.size)
        val putbacks = new UnrolledBuffer [PickledPage]
        for (item <- pages) {
          if (buf.readableBytes + item.byteSize < bytes) {
            val start = buf.writePos
            item.write (buf)
            buf.writePos = blockAlignUp (buf.writePos)
            val pagePos = Position (id, pos + start, buf.writePos - start)
            tally.alloc (item, pagePos.length)
            callbacks.add (item.cb map (_ => pagePos))
          } else {
            putbacks += item
          }}

        // Double check to avoid writing over the segment boundary.
        assert (buf.readableBytes + pos <= seg.limit)

        val alloc: Option [SegmentBounds] =

          // If all pages fit with room to spare.
          if (putbacks.isEmpty && buf.writePos < bytes) {

            None

          // If pages filled or overflowed this segment.
          } else {

            // Return the mis-fits to be flushed latter.
            dispatcher.send (putbacks)

            // Allocate a new segment.
            Some (this.alloc.alloc())
          }

        for {
          _ <- file.flush (buf, pos)
          _ <- ledger.alloc (id, seg.num, pos + buf.writePos, tally)
        } yield {

          // Setup for the next batch.
          alloc match {

            case Some (seg) =>

              // Start fresh for the new segment.
              this.seg = seg
              pos = seg.base

            case None =>

              // Advance the position in this segment for the next batch.
              pos += buf.writePos
          }

          // Acknowledge the writes.
          for (cb <- callbacks)
            scheduler.pass (cb, ())

          // Can we enroll for another batch?
          detacher.finishFlush()
        }}}

  def launch (ledger: SegmentLedger, pos: Long) {
    this.ledger = ledger
    if (geom.isSegmentAligned (pos)) {
      seg = alloc.alloc()
      this.pos = seg.base
    } else {
      seg = alloc.alloc (geom.segmentNum (pos))
      this.pos = pos
    }
    register()
  }

  def startDraining(): Async [Unit] =
    for {
      _ <- detacher.startDraining()
    } yield {
      if (pos == seg.base)
        alloc.free (seg.num)
      seg = SegmentBounds (-1, 0, 0)
    }

  def getSegment(): Int =
    seg.num
}
