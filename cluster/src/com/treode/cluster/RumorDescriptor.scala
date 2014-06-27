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

package com.treode.cluster

import com.treode.pickle.Pickler

class RumorDescriptor [M] (val id: RumorId, val pmsg: Pickler [M]) {

  def listen (f: (M, Peer) => Any) (implicit c: Cluster): Unit =
    c.listen (this) (f)

  def spread (msg: M) (implicit c: Cluster): Unit =
    c.spread (this) (msg)

  override def toString = s"RumorDescriptor($id)"
}

object RumorDescriptor {

  def apply [M] (id: RumorId, pval: Pickler [M]): RumorDescriptor [M] =
    new RumorDescriptor (id, pval)
}
