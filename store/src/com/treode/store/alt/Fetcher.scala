package com.treode.store.alt

import com.treode.async.Async
import com.treode.store.ReadOp

/** Builds a collection of rows to prefetch. This works with [[Transaction]] to prefetch rows from
  * the database into the transaction's cache. This class makes it easy to fetch many rows from
  * different tables in a single call.
  */
class Fetcher private [alt] (tx: Transaction) {

  private val ops = Set.newBuilder [ReadOp]

  def fetch (ops: Iterable [ReadOp]): Fetcher = {
    this.ops ++= ops
    this
  }

  def when (cond: Boolean) (op: => ReadOp): Fetcher = {
    if (cond)
      this.ops += op
    this
  }

  def fetch [K] (desc: TableDescriptor [K, _]): Fetcher.Keys [K] =
    new Fetcher.Keys (this, desc)

  def async(): Async [Unit] =
    tx.fetch (ops.result.toSeq)

  def await(): Unit =
    async().await()
}

object Fetcher {

  class Keys [K] private [Fetcher] (fetcher: Fetcher, desc: TableDescriptor [K, _]) {

    def apply (keys: K*): Fetcher =
      fetcher.fetch (keys map (k => ReadOp (desc.id, desc.key.freeze (k))))

    def apply (keys: Iterable [K]): Fetcher =
      fetcher.fetch (keys map (k => ReadOp (desc.id, desc.key.freeze (k))))

    def when (cond: Boolean) (key: => K) : Fetcher =
      fetcher.when (cond) (ReadOp (desc.id, desc.key.freeze (key)))
  }}
