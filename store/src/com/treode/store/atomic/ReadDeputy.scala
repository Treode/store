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

import scala.util.{Failure, Success}

import com.treode.async.Callback
import com.treode.cluster.{IgnoreRequestException, RequestDescriptor}
import com.treode.store.{ReadOp, TxClock, Value}

private class ReadDeputy (kit: AtomicKit) {
  import kit.{cluster, tables}
  import kit.library.atlas

  def attach() {
    ReadDeputy.read.listen { case ((version, rt, ops), from) =>
      if (atlas.version - 1 <= version && version <= atlas.version + 1)
        tables.read (rt, ops)
      else
        throw new IgnoreRequestException
    }}}

private object ReadDeputy {

  val read = {
    import AtomicPicklers._
    RequestDescriptor (0xFFC4651219C779C3L, tuple (uint, txClock, seq (readOp)), seq (value))
  }}
