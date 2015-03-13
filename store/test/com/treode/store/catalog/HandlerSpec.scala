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

import java.nio.file.Paths
import scala.collection.JavaConversions
import scala.util.Random

import org.scalatest.FreeSpec
import com.treode.async.stubs.StubScheduler
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.implicits._
import com.treode.disk.stubs.{StubDisk, StubDiskDrive}
import com.treode.pickle.Picklers
import com.treode.store.{Bytes, CatalogId, StoreTestConfig, StoreTestTools}

import JavaConversions._

class HandlerSpec extends FreeSpec {

  val ID = CatalogId (0x26)

  val values = Seq (
      0x292C28335A06E344L, 0xB58E76CED969A4C7L, 0xDF20D7F2B8C33B9EL, 0x63D5DAAF0C58D041L,
      0x834727637190788AL, 0x2AE35ADAA804CE32L, 0xE3AA9CFF24BC92DAL, 0xCE33BD811236E7ADL,
      0x7FAF87891BE9818AL, 0x3C15A9283BDFBA51L, 0xE8E45A575513FA90L, 0xE224EF2739907F79L,
      0xFC275E6C532CB3CBL, 0x40C505971288B2DDL, 0xCD1C2FD6707573E1L, 0x2D62491B453DA6A3L,
      0xA079188A7E0C8C39L, 0x46A5B2A69D90533AL, 0xD68C9A2FDAEE951BL, 0x7C781E5CF39A5EB1L)

  val bytes = values map (Bytes (_))

  val patches = {
    var prev = Bytes.empty
    for (v <- bytes) yield {
      val p = Patch.diff (prev, v)
      prev = v
      p
    }}

  private def newCatalog (issues: Int): Handler = {
    val config = StoreTestConfig()
    import config._
    implicit val random = new Random (0)
    val diskDrive = new StubDiskDrive
    implicit val scheduler = StubScheduler.random (random)
    implicit val disk = StubDisk
        .recover()
        .reattach (diskDrive)
        .expectPass()
        .disk
    val cat = Handler (0)
    for ((v, i) <- values.take (issues) .zipWithIndex)
      cat.patch (cat.diff (i+1, Bytes (Picklers.fixedLong, v)))
    cat
  }

  "When computing and apply an update" - {

    "and this catalog has no history" - {

      "and the other catalog has no history, there should be no updates" in {
        val c = newCatalog (0)
        val u = c.diff (0)
        assert (u.isEmpty)
        val c2 = newCatalog (0)
        c2.patch (u)
        assertResult (0) (c2.version)
        assertResult (Bytes.empty) (c2.bytes)
        assertResult (Seq.empty) (c2.history.toSeq)
      }

      "and the other catalog is ahead, there should be no updates" in {
        val c = newCatalog (0)
        assert (c.diff (8) .isEmpty)
      }}

    "and this catalog has history" - {

      "and the other catalog is caught up, there should be no updates" in {
        val c = newCatalog (8)
        assert (c.diff (8) .isEmpty)
      }

      "and the other catalog is ahead, there should be no updates" in {
        val c = newCatalog (8)
        assert (c.diff (12) .isEmpty)
      }

      "and the other catalog is not too far behind" - {

        "and it can retain all its history, it should work" in {
          val c = newCatalog (12)
          assertResult (bytes (11)) (c.bytes)
          val u @ Patch (ver, sum, ps) = c.diff (8)
          assertResult (12) (ver)
          assertResult (bytes (11) .murmur32) (sum)
          assertResult (patches drop 8 take 4) (ps)
          val c2 = newCatalog (8)
          assertResult (bytes (7)) (c2.bytes)
          c2.patch (u)
          assertResult (12) (c2.version)
          assertResult (bytes (11)) (c2.bytes)
          assertResult (patches take 12) (c2.history.toSeq)
        }

        "and it can drop some of its history, it should work" in {
          val c = newCatalog (20)
          assertResult (bytes (19)) (c.bytes)
          val u @ Patch (ver, sum, ps) = c.diff (18)
          assertResult (20) (ver)
          assertResult (bytes (19) .murmur32) (sum)
          assertResult (patches drop 18 take 2) (ps)
          val c2 = newCatalog (18)
          assertResult (bytes (17)) (c2.bytes)
          assertResult (16) (c2.history.size)
          c2.patch (u)
          assertResult (20) (c2.version)
          assertResult (bytes (19)) (c2.bytes)
          assertResult (patches drop 4) (c2.history.toSeq)
        }}

      "and the other catalog is far behind" - {

        "it should synchronize the full catalog" in {
          val c = newCatalog (20)
          val u @ Assign (v, b, ps) = c.diff (0)
          assertResult (20) (v)
          assertResult (bytes (19)) (c.bytes)
          assertResult (patches drop 4) (ps)
          val c2 = newCatalog (0)
          c2.patch (u)
          assertResult (20) (c2.version)
          assertResult (bytes (19)) (c2.bytes)
          assertResult (patches drop 4) (c2.history.toSeq)
        }}}}}
