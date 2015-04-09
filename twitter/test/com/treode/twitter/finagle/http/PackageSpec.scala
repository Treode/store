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

import com.treode.store.Slice
import com.twitter.finagle.http.Request
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
  }}
