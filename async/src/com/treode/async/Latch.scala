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

/** Await the completion of many asynchronous operations, perhaps collecting the results.
  */
object Latch {

  /** Create a callback that takes a value, and invoke the `cb` after collecting `count` results.
    * The elements in the result are ordered according to how they arrived.
    */
  def casual [A] (count: Int, cb: Callback [Seq [A]]) (implicit m: Manifest [A]): Callback [A] =
    new CasualLatch (count, cb)

  /** Create a callback that takes a key and value, and invoke the `cb` after collecting `count`
    * pairs.
    */
  def map [K, V] (count: Int, cb: Callback [Map [K, V]]): Callback [(K, V)] =
    new MapLatch (count, cb)

  /** Create a callback that takes an index and value, and invoke the `cb` after collecting
    * `count` results.  The elements in the result are ordered according to the provided index.
    */
  def seq [A] (count: Int, cb: Callback [Seq [A]]) (implicit m: Manifest [A]): Callback [(Int, A)] =
    new ArrayLatch (count, cb)

  /** Create a callback that takes nothing, and invoke the `cb` after the callback is invoked
    * `count` times.
    */
  def unit [A] (count: Int, cb: Callback [Unit]): Callback [A] =
    new CountingLatch [A] (count, cb)
}
