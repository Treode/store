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

/** A key together with a timestamp; sorts in reverse chronological order. */
case class Key (key: Bytes, time: TxClock) extends Ordered [Key] {

  def key [K] (p: Pickler [K]): K =
    key.unpickle (p)

  def compare (that: Key): Int = {
    val r = key compare that.key
    if (r != 0) return r
    // Reverse chronological order
    that.time compare time
  }}

object Key extends Ordering [Key] {

  val MinValue = Key (Bytes.MinValue, TxClock.MaxValue)

  def apply [K] (pk: Pickler [K], key: K, time: TxClock): Key =
    Key (Bytes (pk, key), time)

  def compare (x: Key, y: Key): Int =
    x compare y

  val pickler = {
    import StorePicklers._
    wrap (bytes, txClock)
    .build (v => new Key (v._1, v._2))
    .inspect (v => (v.key, v.time))
  }}
