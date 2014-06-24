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

package com.treode.store.tier

class TableTracker {

  private var attempted = Map.empty [Int, Option [Int]] .withDefaultValue (None)
  private var accepted = Map.empty [Int, Option [Int]] .withDefaultValue (None)

  def putting (key: Int, value: Int): Unit =
    attempted += key -> Some (value)

  def put (key: Int, value: Int): Unit =
    accepted += key -> Some (value)

  def deleting (key: Int): Unit =
    attempted += key -> None

  def deleted (key: Int): Unit =
    accepted += key -> None

  def check (recovered: Map [Int, Int]) {
    for (k <- accepted.keySet)
      assert (
          recovered.contains (k) || attempted (k) == None,
          s"Expected $k to be recovered")
    for ((k, v) <- recovered) {
      val expected = attempted (k) .toSet ++ accepted (k) .toSet
      assert (expected contains v,
          s"Expected $k to be ${expected mkString " or "}, found $v")
    }}}
