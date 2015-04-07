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

package com.treode.disk

import java.util.ArrayList

import com.treode.async.Async, Async.guard
import com.treode.async.implicits._

/** A convenient facade for an immutable sequence of checkpoint methods. */
private class CheckpointerRegistry (checkpointers: ArrayList [Unit => Async [Unit]]) {

  /** Run each checkpoint in turn. */
  def checkpoint(): Async [Unit] =
    guard {
      checkpointers.latch (_(()))
    }}

private object CheckpointerRegistry {

  /** Collect checkpoint methods to build a registry; not thread safe. */
  class Builder {

    private val checkpointers = new ArrayList [Unit => Async [Unit]]

    def add (f: => Async [Unit]): Unit =
        checkpointers.add (_ => f)

    def result: CheckpointerRegistry =
      new CheckpointerRegistry (checkpointers)
  }}
