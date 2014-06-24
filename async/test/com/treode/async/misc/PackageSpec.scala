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

package com.treode.async.misc

import org.scalatest.FlatSpec

class PackageSpec extends FlatSpec {

  "parseUnsignedLong" should "work" in {
    assertResult (Some (0xFECDB005DA3B9A60L)) (parseUnsignedLong ("0xFECDB005DA3B9A60"))
    assertResult (Some (0xFECDB005DA3B9A60L)) (parseUnsignedLong ("#FECDB005DA3B9A60"))
    assertResult (Some (64)) (parseUnsignedLong ("0100"))
    assertResult (Some (10L)) (parseUnsignedLong ("10"))
    assertResult (Some (-1)) (parseUnsignedLong ("0xFFFFFFFFFFFFFFFF"))
    assertResult (None) (parseUnsignedLong ("0x1FFFFFFFFFFFFFFFF"))
  }}
