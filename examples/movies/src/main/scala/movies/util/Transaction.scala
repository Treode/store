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

package movies.util

import com.treode.async.Async
import com.treode.store.{Key => _, Value => _, _}

import Async.{supply, when}
import Transaction._
import WriteOp._

/** A Transaction mediates interaction with the database. This class support optimistic
  * transactions. To begin, just create a new Transaction object; there is no need to `begin` one
  * on some database connection, no database resources are opened, and no locks are held.
  *
  * As you read, this object caches the items. As you write, this object tracks those items
  * so that a latter read will see the effect; only your reads through this object will see it.
  * When you complete the work and wish to commit the writes, call `execute`. This will succeed
  * only if none of the read items have been updated since they were read. You will need to
  * check the write result for a [[com.treode.store.StaleException StaleException]]; in that case
  * you will need to restart the work.
  *
  * If you encounter a failure while processing, just throw an exception. There's no need to
  * `rollback` the transaction.
  *
  * This object is not thread safe.
  */
class Transaction (rt: TxClock) (implicit store: Store) {

  private var _cache = Map.empty [Key, Value]
  private var _vt = TxClock.MinValue

  def vt = _vt

  private def _fetch (need: Seq [ReadOp]): Async [Unit] =
    for {
      vs <- store.read (rt, need: _*)
    } yield {
      for ((op, v0) <- need zip vs) {
        if (_vt < v0.time) _vt = v0.time
        val v1 = if (v0.value.isEmpty) NotFound else Found (v0.value.get)
        _cache += Key (op) -> v1
      }
    }

  def fetch (ops: ReadOp*): Async [Unit] = {
    val need = ops filter (op => !(_cache contains (Key (op))))
    when (!need.isEmpty) (_fetch (need))
  }

  def get (id: TableId, key: Bytes): Option [Bytes] =
    _cache (Key (id, key)) match {
      case NotFound        => None
      case Found (value)   => Some (value)
      case Created (value) => Some (value)
      case Updated (value) => Some (value)
      case Deleted         => None
    }

  def create (id: TableId, key: Bytes, value: Bytes): Unit = {
    val _key = Key (id, key)
    _cache get _key match {
      case Some (NotFound) => _cache += _key -> Created (value)
      case Some (Deleted)  => _cache += _key -> Updated (value)
      case None            => _cache += _key -> Created (value)
      case _               => throw new Exception (s"Row ${_key} already exists.")
    }}

  def update (id: TableId, key: Bytes, value: Bytes): Unit =
    _cache += Key (id, key) -> Updated (value)

  def delete (id: TableId, key: Bytes): Unit =
    _cache += Key (id, key) -> Deleted

  def execute (xid: TxId): Async [TxClock] = {
    val ops = _cache.toSeq map {
      case (k, NotFound)    => Hold (k.table, k.key)
      case (k, Found (v))   => Hold (k.table, k.key)
      case (k, Created (v)) => Create (k.table, k.key, v)
      case (k, Updated (v)) => Update (k.table, k.key, v)
      case (k, Deleted)     => Delete (k.table, k.key)
    }
    store.write (xid, rt, ops: _*)
  }}

object Transaction {

  private [Transaction] case class Key (table: TableId, key: Bytes)

  private [Transaction] object Key {

    def apply (op: ReadOp): Key = Key (op.table, op.key)
  }

  sealed abstract class Value

  case object NotFound extends Value

  case class Found (value: Bytes) extends Value

  case class Created (value: Bytes) extends Value

  case class Updated (value: Bytes) extends Value

  case object Deleted extends Value
}
