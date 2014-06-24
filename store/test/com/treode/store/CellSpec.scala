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

package com.treode.store

import org.scalatest.FlatSpec

import Fruits.{Apple, Orange}
import StoreTestTools._

class CellSpec extends FlatSpec {

  "Cell.compare" should "sort by key" in {
    assert (Apple##1 < Orange##1)
    assert ((Apple##1 compare Apple##1) == 0)
  }

  it should "reverse sort by time" in {
    assert (Apple##2 < Apple##1)
  }}
