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

package com.treode.store.alt

import com.treode.async.{Async, AsyncIterator}
import com.treode.store._

import WriteOp._

/** A Transaction mediates interaction with the database. This class supports optimistic
  * transactions. It maintains its own view of the database plus its changes. To begin a
  * transaction, simply create a new Transaction object; there is no need to `begin` one on some
  * database connection, no database resources are opened, and no locks are held.
  *
  * As you read, the transaction caches the items. As you write, it tracks them so that later reads
  * will see the effects. Only your reads through this transaction will see the writes through this
  * transaction. When you complete the work, call `execute` to commit the changes. This will
  * succeed only if none of the items have been updated since they were read. You will need to
  * check the write result for a [[StaleException]]; in that case you will need to restart the
  * work.
  *
  * If you encounter a failure while processing, just throw an exception. There's no need to
  * `rollback` the transaction.
  *
  * This object is not thread safe.
  */
class Transaction (rt: TxClock) (implicit store: Store) {

  private case class Key (table: TableId, key: Bytes)

  private object Key {

    def apply (op: ReadOp): Key = Key (op.table, op.key)
  }

  private sealed abstract class Value

  private case object NotFound extends Value

  private case class Found (value: Bytes) extends Value

  private case class Created (value: Bytes) extends Value

  private case class Updated (value: Bytes) extends Value

  private case object Deleted extends Value

  private var _cache = Map.empty [Key, Value]
  private var _vt = TxClock.MinValue

  def vt = _vt

  /** Prefetch rows from the database into this transaction's cache. */
  def fetch (ops: Seq [ReadOp]): Async [Unit] = {
    val need = ops filter (op => !(_cache contains (Key (op))))
    Async.when (!need.isEmpty) {
      for {
        vs <- store.read (rt, need: _*)
      } yield {
        for ((op, v0) <- need zip vs) {
          if (_vt < v0.time) _vt = v0.time
          val v1 = if (v0.value.isEmpty) NotFound else Found (v0.value.get)
          _cache += Key (op) -> v1
        }}}}

  def when (cond: Boolean) (op: => ReadOp): Async [Unit] =
    Async.when (cond) (fetch (Seq (op)))

  /** Prefetch rows from the database into this transaction's cache. */
  def fetch [K] (desc: TableDescriptor [K, _]): Transaction.Keys [K] =
    new Transaction.Keys (this, desc)

  /** Create a fetcher to prefetch different kinds of rows. */
  def fetcher: Fetcher = new Fetcher (this)

  def recent [K] (desc: TableDescriptor [K, _], start: K): AsyncIterator [desc.Cell] =
    desc.recent (rt, start)

  def recent [K] (desc: TableDescriptor [K, _]): AsyncIterator [desc.Cell] =
    desc.recent (rt)

  /** Get a row from this transaction's cache. Call `fetch` to prefetch rows from the database
    * into the cache before calling `get`.
    */
  def get (id: TableId, key: Bytes): Option [Bytes] =
    _cache (Key (id, key)) match {
      case NotFound        => None
      case Found (value)   => Some (value)
      case Created (value) => Some (value)
      case Updated (value) => Some (value)
      case Deleted         => None
    }

  /** Get a row from this transaction's cache. Call `fetch` to prefetch rows from the database
    * into the cache before calling `get`.
    */
  def get [K, V] (d: TableDescriptor [K, V]) (k: K): Option [V] =
    get (d.id, d.key.freeze (k)) .map (d.value.thaw (_))

  def create (id: TableId, key: Bytes, value: Bytes): Unit = {
    val _key = Key (id, key)
    _cache get _key match {
      case Some (NotFound) => _cache += _key -> Created (value)
      case Some (Deleted)  => _cache += _key -> Updated (value)
      case None            => _cache += _key -> Created (value)
      case _               => throw new Exception (s"Row ${_key} already exists.")
    }}

  def create [K, V] (d: TableDescriptor [K, V]) (k: K, v: V): Unit =
    create (d.id, d.key.freeze (k), d.value.freeze (v))

  def update (id: TableId, key: Bytes, value: Bytes): Unit =
    _cache += Key (id, key) -> Updated (value)

  def update [K, V] (d: TableDescriptor [K, V]) (k: K, v: V): Unit =
    update (d.id, d.key.freeze (k), d.value.freeze (v))

  def delete (id: TableId, key: Bytes): Unit =
    _cache += Key (id, key) -> Deleted

  def delete [K] (d: TableDescriptor [K, _]) (k: K): Unit =
    delete (d.id, d.key.freeze (k))

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

  class Keys [K] private [Transaction] (tx: Transaction, desc: TableDescriptor [K, _]) {

    def apply (keys: K*): Async [Unit] =
      tx.fetch (keys map (k => ReadOp (desc.id, desc.key.freeze (k))))

    def apply (keys: Iterable [K]): Async [Unit] =
      tx.fetch (keys.map (k => ReadOp (desc.id, desc.key.freeze (k))) .toSeq)

    def when (cond: Boolean) (key: => K): Async [Unit] =
      tx.when (cond) (ReadOp (desc.id, desc.key.freeze (key)))
  }}
