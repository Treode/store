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

package movies

import com.treode.async.Async
import com.treode.store.ReadOp

package object util {

  implicit def pairToReadOp [K, V] (pair: (TableDescriptor [K, V], K)): ReadOp = {
    val (d, k) = pair
    ReadOp (d.id, d.k.freeze (k))
  }

  /** This implicit class extends [[Transaction]] to link it with the [[TableDescriptor]] and
    * provides type-safe methods for reading and writing the database.
    *
    * ```
    * // MyTable has ID 0x123 and maps strings to strings.
    * val MyTable = TableDescriptor (0x123, Frost.strings, Frost.string)
    *
    * // Start a transaction as-of now.
    * val tx = new Transaction (TxClock.now)
    *
    * // Write row "message" of MyTable.
    * tx.write (MyTable) ("message", "Hello World")
    *
    * // Read row "message" of MyTable; s will have type String.
    * val s = tx.read (MyTable) ("message")
    * ```
    */
  implicit class RichTransaction (tx: Transaction) {

    def fetch [K] (desc: TableDescriptor [K, _]) (keys: Seq [K]): Async [Unit] = {
      val ops = keys map (k => ReadOp (desc.id, desc.k.freeze (k)))
      tx.fetch (ops: _*)
    }

    def get [K, V] (d: TableDescriptor [K, V]) (k: K): Option [V] =
      tx.get (d.id, d.k.freeze (k)) .map (d.v.thaw (_))

    def create [K, V] (d: TableDescriptor [K, V]) (k: K, v: V): Unit =
      tx.create (d.id, d.k.freeze (k), d.v.freeze (v))

    def update [K, V] (d: TableDescriptor [K, V]) (k: K, v: V): Unit =
      tx.update (d.id, d.k.freeze (k), d.v.freeze (v))

    def delete [K] (d: TableDescriptor [K, _]) (k: K): Unit =
      tx.delete (d.id, d.k.freeze (k))
  }}
