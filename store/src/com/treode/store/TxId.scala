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
import org.joda.time.Instant

/** A transaction identifier; an arbitrary array of bytes and a time.
  *
  * A transaction identifier must be universally unique.  The `time` does not play into Treode's
  * consistency mechanism; it exists only so that the database may cleanup old statuses.  When
  * issuing a write, the time of the transaction should be near the actual time of the write for
  * your sake, not for correctness of the transaction mechanism.  When a transaction ages past
  * `retention` in [[Store.Config]], the database will remove the status record.
  */
case class TxId (id: Bytes, time: Instant) extends Ordered [TxId] {

  def compare (that: TxId): Int = {
    val r = id compare that.id
    if (r != 0) return r
    time.getMillis compare that.time.getMillis
  }

  /** The TxID as `<hex>:<ISO-8601>`. */
  override def toString: String =
    f"${id.toHexString}:${time.toString}"
}

object TxId extends Ordering [TxId] {

  private val _random = {
    import StorePicklers._
    tuple (hostId, fixedLong)
  }

  val MinValue = TxId (Bytes.MinValue, 0)

  def apply (id: Bytes, time: Long): TxId =
    TxId (id, new Instant (time))

  def random (host: HostId): TxId =
    TxId (Bytes (_random, (host, Random.nextLong)), Instant.now)

  /** Parses `<hex>:<ISO-8601>`. */
  def parse (s: String): Option [TxId] = {
    s.split (':') match {
      case Array (_id, _time) =>
        Some (new TxId (Bytes.fromHexString (_id), Instant.parse (_time)))
      case _ =>
        None
    }}

  def compare (x: TxId, y: TxId): Int =
    x compare y

  val pickler = {
    import StorePicklers._
    wrap (bytes, instant)
    .build (v => new TxId (v._1, v._2))
    .inspect (v => (v.id, v.time))
  }}
