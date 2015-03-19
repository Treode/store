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

/** A convenient facade for a `Map [Int, Set [Int]]`, that is a
  * `Map [DiskNumber, Set [SegmentNumber]]`.
  *
  * When the user releases pages, if those are the last pages occupying a segment of disk space,
  * then that segment may be reclaimed and reused. The `SegmentLedger` performs this check and
  * returns the newly available segments as a `SegmentDocket`.
  */
private case class SegmentDocket private (private var segs: Map [Int, Set [Int]])
extends Iterable [(Int, Set [Int])] with Traversable [(Int, Set [Int])] {

  def this() = this (Map.empty.withDefaultValue (Set.empty))

  def apply (disk: Int): Set [Int] =
    segs (disk)

  def add (disk: Int, seg: Int): Unit =
    segs += disk -> (segs (disk) + seg)

  def add (disk: Int, segs: Iterable [Int]): Unit =
    this.segs += disk -> (this.segs (disk) ++ segs)

  def remove (disk: Int, seg: Int): Unit =
    segs get (disk) match {
      case Some (s0) =>
        val s1 = s0 - seg
        segs = if (s1.isEmpty) segs - disk else segs + (disk -> s1)
      case None => ()
    }

  def remove (disk: Int, segs: Iterable [Int]): Unit =
    this.segs get (disk) match {
      case Some (s0) =>
        val s1 = s0 -- segs
        this.segs = if (s1.isEmpty) this.segs - disk else this.segs + (disk -> s1)
      case None => ()
    }

  def iterator = segs.iterator

  override def foreach [U] (f: ((Int, Set [Int])) => U) = segs.foreach (f)

  override def toString = s"SegmentDocket(${segs mkString ","})"
}

private object SegmentDocket {

  val empty = SegmentDocket (Map.empty)
}
