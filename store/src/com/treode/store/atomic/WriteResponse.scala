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

package com.treode.store.atomic

import com.treode.store.TxClock

private sealed abstract class WriteResponse

private object WriteResponse {

  case class Prepared (ft: TxClock) extends WriteResponse
  case class Collisions (ks: Set [Int]) extends WriteResponse
  case class Advance (time: TxClock) extends WriteResponse
  case object Committed extends WriteResponse
  case object Aborted extends WriteResponse
  case object Failed extends WriteResponse

  val pickler = {
    import AtomicPicklers._
    tagged [WriteResponse] (
        0x1 -> wrap (txClock) .build (Prepared.apply _) .inspect (_.ft),
        0x2 -> wrap (set (uint)) .build (Collisions.apply _) .inspect (_.ks),
        0x3 -> wrap (txClock) .build (Advance.apply _) .inspect (_.time),
        0x4 -> const (Committed),
        0x5 -> const (Aborted),
        0x6 -> const (Failed))
  }}
