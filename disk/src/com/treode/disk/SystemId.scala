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

import com.treode.pickle.Picklers

case class SystemId (id1: Long, id2: Long) {

  override def toString =
    if (id1 < 256 && id2 < 256)
      f"System:$id1%02X:$id2%02X"
    else
      f"System:$id1%016X:$id2%016X"
}

object SystemId {

  val pickler = {
    import Picklers._
    wrap (fixedLong, fixedLong)
    .build ((apply _).tupled)
    .inspect (v => (v.id1, v.id2))
  }}
