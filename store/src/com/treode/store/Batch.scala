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

import com.treode.pickle.Pickler

/** Chop a table scan into batches.
  *
  * Limit the batch of a table scan to be a maximum number of elements, or a maximum number of
  * bytes, whichever limit is hit first. A batch will include at least one element, regardless of
  * its byte size.
  */
case class Batch (entries: Int, bytes: Int)

object Batch {

  val suggested = new Batch (1000, 1 << 16)

  val pickler = {
    import StorePicklers._
    wrap (tuple (int, int))
    .build (v => Batch (v._1, v._2))
    .inspect (v => (v.entries, v.bytes))
  }}
