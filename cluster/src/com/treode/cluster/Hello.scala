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

import com.treode.pickle.Picklers

private case class Hello (host: HostId, cell: CellId)

private object Hello {

  val pickler = {
    import ClusterPicklers._
    tagged [Hello] (
      0x1 -> wrap (tuple (hostId, cellId))
          .build (v => new Hello (v._1, v._2))
          .inspect (v => (v.host, v.cell)))
  }}
