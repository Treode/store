package com.treode.store.alt

import com.treode.async.Async
import com.treode.store.ReadOp

/** Builds a collection of rows to prefetch. This works with [[Transaciton]] to prefetch rows from
  * the database into the transaction's cache. This class makes it easy to fetch many rows from
  * different tables in a single call.
  */
class Fetcher private [alt] (tx: Transaction) {

  private val ops = Set.newBuilder [ReadOp]

  def fetch [K] (desc: TableDescriptor [K, _]) (keys: K*): Fetcher = {
    ops ++= keys map (k => ReadOp (desc.id, desc.key.freeze (k)))
    this
  }

  def when [K] (cond: Boolean) (desc: TableDescriptor [K, _]) (keys: K*): Fetcher = {
    if (cond)
      ops ++= keys map (k => ReadOp (desc.id, desc.key.freeze (k)))
    this
  }

  def async(): Async [Unit] =
    tx.fetch (ops.result.toSeq)
  
  def await(): Unit =
    async().await()
}
