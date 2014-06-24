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

package com.treode.store.catalog

import com.nothome.delta.{Delta, GDiffPatcher}
import com.treode.store.Bytes

private sealed abstract class Update {

  def version: Int
  def checksum: Int
  def isEmpty: Boolean
}

private case class Patch (version: Int, checksum: Int, patches: Seq [Bytes]) extends Update {

  def isEmpty: Boolean = patches.isEmpty
}

private object Patch {

  def diff (v0: Bytes, v1: Bytes): Bytes = {
    val differ = new Delta
    differ.setChunkSize (catalogChunkSize)
    Bytes (differ.compute (v0.bytes, v1.bytes))
  }

  def patch (v0: Bytes, p: Bytes): Bytes = {
    val patcher = new GDiffPatcher
    Bytes (patcher.patch (v0.bytes, p.bytes))
  }

  val pickler = {
    import CatalogPicklers._
    wrap (uint, fixedInt, seq (bytes))
    .build (v => Patch (v._1, v._2, v._3))
    .inspect (v => (v.version, v.checksum, v.patches))
  }}

private case class Assign (version: Int, bytes: Bytes, history: Seq [Bytes]) extends Update {

  def checksum = bytes.murmur32
  def isEmpty: Boolean = false
}

private object Assign {

  val pickler = {
    import CatalogPicklers._
    wrap (uint, bytes, seq (bytes))
    .build (v => Assign (v._1, v._2, v._3))
    .inspect (v => (v.version, v.bytes, v.history))
  }}

private object Update {

  val empty = Patch (0, 0, Seq.empty)

  val pickler = {
    import CatalogPicklers._
    tagged [Update] (
        0x1 -> patch,
        0x2 -> assign)
  }}
