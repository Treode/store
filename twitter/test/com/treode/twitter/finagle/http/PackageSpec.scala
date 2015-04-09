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

package com.treode.twitter.finagle.http

import scala.language.implicitConversions

import com.twitter.finagle.http.Request
import com.treode.store.{Bound, Slice, TxClock, Window}, Window._
import com.twitter.finagle.http.Request
import org.joda.time.Instant
import org.scalatest.FlatSpec

class PackageSpec extends FlatSpec {

  def checkSlice (expected: Slice) (params: (String, String)*) {
    val req = Request (params: _*)
    assertResult (expected) (req.slice)
  }

  def checkSlice (msg: String) (params: (String, String)*) {
    val req = Request (params: _*)
    val ex = intercept [BadRequestException] (req.slice)
    assertResult (msg) (ex.getMessage)
  }

  "Request.slice" should "work" in {
    val together = "Both slice and nslices are needed together."
    val power = "Number of slices must be a power of two and at least one."
    val between = "The slice must be between 0 (inclusive) and the number of slices (exclusive)."
    checkSlice (Slice.all) ()
    checkSlice (Slice (0, 2)) ("slice" -> "0", "nslices" -> "2")
    checkSlice (Slice (1, 2)) ("slice" -> "1", "nslices" -> "2")
    checkSlice ("Bad integer for nslices: abc") ("nslices" -> "abc")
    checkSlice ("Bad integer for slice: abc") ("slice" -> "abc")
    checkSlice (together) ("nslices" -> "2")
    checkSlice (together) ("slice" -> "0")
    checkSlice (power) ("slice" -> "0", "nslices" -> "-1")
    checkSlice (power) ("slice" -> "0", "nslices" -> "3")
    checkSlice (between) ("slice" -> "2", "nslices" -> "2")
    checkSlice (between) ("slice" -> "-1", "nslices" -> "2")
  }

  /** Take a snapshot of TxClock.now for this run of testing. */
  val now = TxClock.now

  implicit def toTxClock (t: Int): TxClock =
    TxClock.parse (t.toString) .get

  /** Allows deviation around now, but not for other times. */
  def relax (time: TxClock): TxClock = {
    val d = 1 << 22 // Allow 16M usec (16 sec) deviation.
    val n = now.time
    val t = time.time
    if (n - d < t  && t < n + d) now else time
  }

  def relax (bound: Bound [TxClock]): Bound [TxClock] =
    bound map (relax (_))

  def relax (window: Window): Window =
    window match {
      case Latest (until, since) => Latest (relax (until), relax (since))
      case Between (until, since) => Between (relax (until), relax (since))
      case Through (until, since) => Through (relax (until), relax (since))
    }

  def checkWindow (expected: Window) (params: (String, String)*) {
    val req = Request (params: _*)
    assertResult (expected) (relax (req.window))
  }

  def checkWindow (msg: String) (params: (String, String)*) {
    val req = Request (params: _*)
    val ex = intercept [BadRequestException] (relax (req.window))
    assertResult (msg) (ex.getMessage)
  }

  "Request.window" should "work" in {
    checkWindow (Latest (now, true, 0, false)) ()
    checkWindow (Latest (now, true, 1, false)) ("since" -> "1")
    checkWindow (Latest (1, true, 0, false)) ("until" -> "1")
    checkWindow (Latest (now, true, 0, false)) ("pick" -> "latest")
    checkWindow (Between (now, true, 0, false)) ("pick" -> "between")
    checkWindow (Through (now, true, 0)) ("pick" -> "through")
    checkWindow (Between (now, true, 0, false)) ("pick" -> "BeTwEeN")
    checkWindow ("Since must preceed until.") ("since" -> "2", "until" -> "1")
    checkWindow ("Pick must be latest, between or through.") ("pick" -> "foo")
  }}
