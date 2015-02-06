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

import com.treode.async.Async
import com.treode.async.implicits._

import Async.guard

private class CheckpointRegistry {

  private val checkpoints = new ArrayList [Unit => Async [Unit]]

  def checkpoint (f: => Async [Unit]): Unit =
    checkpoints.add (_ => f )

  def checkpoint(): Async [Unit] =
    guard {
      checkpoints.latch (_(()))
    }}
