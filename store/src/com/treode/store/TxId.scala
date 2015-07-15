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

import java.math.BigInteger
import scala.util.Random

import com.treode.cluster.HostId

/** A transaction identifier; an arbitrary array of bytes and a time.
  *
  * A transaction identifier must be universally unique.  The `time` does not play into Treode's
  * consistency mechanism; it exists only so that the database may cleanup old statuses.  When
  * issuing a write, the time of the transaction should be near the actual time of the write for
  * your sake, not for correctness of the transaction mechanism.  When a transaction ages past
  * `retention` in [[Store.Config]], the database will remove the status record.
  */
case class TxId (id: Bytes) extends Ordered [TxId] {

  def compare (that: TxId): Int =
    id compare that.id

  /** The TxId hashed to two hex digits. */
  def toShortString: String =
    f"${hashCode & 0xFF}%02X"

  /** The TxId as hexadecimal. */
  override def toString: String =
    id.toHexString
}

object TxId extends Ordering [TxId] {

  private val _random = {
    import StorePicklers._
    tuple (fixedLong, fixedLong, fixedInt)
  }

  val MinValue = TxId (Bytes.MinValue)

  /** Random 160 bit value. */
  def random: TxId =
    TxId (Bytes (_random, (Random.nextLong, Random.nextLong, Random.nextInt)))

  /** Parses hexadecimal. */
  def parse (s: String): Option [TxId] =
    Some (new TxId (Bytes.fromHexString (s)))

  def compare (x: TxId, y: TxId): Int =
    x compare y

  val pickler = {
    import StorePicklers._
    wrap (bytes)
    .build (new TxId (_))
    .inspect (_.id)
  }}
