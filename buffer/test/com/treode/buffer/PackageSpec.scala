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

package com.treode.buffer

import org.scalatest.FlatSpec

class PackageSpec extends FlatSpec {

  def twopow (in: Int, out: Int) {
    it should (s"yield $out for $in") in {
      assertResult (out) (com.treode.buffer.twopow (in))
    }}

  behavior of "PagedBuffer.twopow"
  twopow (0, 1)
  twopow (1, 2)
  twopow (2, 4)
  twopow (3, 4)
  twopow (4, 8)
  twopow (5, 8)
  twopow (7, 8)
  twopow (8, 16)
  twopow (9, 16)
  twopow (15, 16)
  twopow (16, 32)
  twopow (17, 32)
  twopow (31, 32)
  twopow (32, 64)
}
