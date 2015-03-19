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

import org.scalatest.FlatSpec

class AllocationDocketSpec extends FlatSpec {

  "freeing no groups" should "free no disks or segments" in {
    // Allocate group 0 on (disk 0, segs 0 and 1) and on (disk 1, seg 0)
    // Allocate group 1 on (disk 0, seg 1)
    // Free no groups, expect no disks or segments to be freed
    val docket = new AllocationDocket
    docket.alloc (0, 0, 0, 0, 0, 1)
    docket.alloc (0, 0, 0, 1, 0, 1)
    docket.alloc (0, 0, 1, 0, 1, 1)
    assert (SegmentDocket.empty == docket.free (0, 0, Set.empty))
  }

  "freeing groups" should "free disks and segments" in {
    // Allocate group 0 on (disk 0, segs 0 and 1) and on (disk 1, seg 0)
    // Allocate group 1 on (disk 0, seg 1)
    // Free group 0, expect (disk 0, seg 0) and (disk 1) to be freed
    val docket = new AllocationDocket
    docket.alloc (0, 0, 0, 0, 0, 1)
    docket.alloc (0, 0, 0, 1, 0, 1)
    docket.alloc (0, 0, 1, 0, 1, 1)
    val expected = new SegmentDocket
    expected.add (0, 0)
    expected.add (1, 0)
    assert (expected == docket.free (0, 0, Set (0)))
  }}
