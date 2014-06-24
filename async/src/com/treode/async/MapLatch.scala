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

package com.treode.async

import scala.util.{Failure, Success, Try}

private class MapLatch [K, V] (count: Int, cb: Callback [Map [K, V]])
extends AbstractLatch [Map [K, V]] (count, cb) with Callback [(K, V)] {

  private var map = Map.newBuilder [K, V]
  map.sizeHint (count)

  init()

  def value = map.result

  def apply (v: Try [(K, V)]): Unit = synchronized {
    v match {
      case Success ((k, v)) =>
        map += (k -> v)
        release()
      case Failure (t) => failure (t)
    }}}
