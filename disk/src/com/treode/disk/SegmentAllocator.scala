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

import com.googlecode.javaewah.{EWAHCompressedBitmap => Bitmap}
import com.treode.async.{Async, Callback, Scheduler}, Async.async
import com.treode.disk.exceptions.DiskFullException

import SegmentAllocator.{bitmapOf, bitmapRange}

/** Track free segments and allocate them. */
private class SegmentAllocator (
  geom: DriveGeometry
) (implicit
  scheduler: Scheduler,
  config: DiskConfig
) {

  import geom.{segmentBits, segmentCount}

  private val segmentMin = config.diskLeadBytes >> segmentBits
  private val segmentsUseable = segmentCount - segmentMin

  private var _free: Bitmap = bitmapRange (segmentMin, segmentCount)
  private var drains = List.empty [Callback [Unit]]

  private def isDrained: Boolean =
    _free.cardinality == segmentsUseable

  private def validSegment (n: Int): Boolean =
    n >= segmentMin && n < segmentCount

  def allocated: Int = {
    val nfree = synchronized (_free.cardinality)
    segmentsUseable - nfree
  }

  def alloc(): SegmentBounds = {
    val n = synchronized {
      val n = _free.getFirstSetBit
      if (n < 0)
        throw new DiskFullException
      _free = _free andNot (bitmapOf (n))
      n
    }
    assert (validSegment (n))
    SegmentBounds (n, geom, config)
  }

  def alloc (n: Int): SegmentBounds = {
    require (validSegment (n))
    synchronized (_free = _free andNot (bitmapOf (n)))
    SegmentBounds (n, geom, config)
  }

  def alloc (ns: Set [Int]) {
    require (ns forall (validSegment _))
    synchronized (_free = _free andNot (bitmapOf (ns)))
  }

  private def _drain() {
    if (!drains.isEmpty && isDrained) {
      for (cb <- drains) scheduler.pass (cb, ())
      drains = List.empty
    }}

  def free (n: Int) {
    require (validSegment (n))
    synchronized {
      _free = _free or (bitmapOf (n))
      _drain()
    }}

  def free (ns: Set [Int]) {
    require (ns forall (validSegment _))
    synchronized {
      _free = _free or (bitmapOf (ns))
      _drain()
    }}

  def awaitDrained(): Async [Unit] =
    async { cb =>
      synchronized {
        if (isDrained)
          scheduler.pass (cb, ())
        else
          drains ::= cb
      }}}

private object SegmentAllocator {

  def bitmapOf (n: Int): Bitmap =
    Bitmap.bitmapOf (n)

  def bitmapOf (ns: Set [Int]): Bitmap =
    Bitmap.bitmapOf (ns.toSeq.sorted: _*)

  def bitmapRange (from: Int, to: Int): Bitmap = {
    val upper = new Bitmap
    upper.setSizeInBits (to, true)
    val lower = new Bitmap
    lower.setSizeInBits (from, true)
    upper andNot lower
  }}
