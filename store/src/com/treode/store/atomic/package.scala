package com.treode.store

import java.util.concurrent.ConcurrentHashMap
import com.treode.async.AsyncIterator
import com.treode.store.locks.LockSet
import com.treode.store.tier.{TierMedic, TierTable}

package atomic {

  private sealed abstract class PrepareResult

  private object PrepareResult {
    case class Prepared (vt: TxClock, locks: LockSet) extends PrepareResult
    case class Collided (ks: Seq [Int]) extends PrepareResult
    case object Stale extends PrepareResult
  }}

package object atomic {

  private [atomic] type TablesMap = ConcurrentHashMap [TableId, TierTable]
  private [atomic] type TableMedicsMap = ConcurrentHashMap [TableId, TierMedic]
  private [atomic] type WritersMap = ConcurrentHashMap [TxId, WriteDeputy]
  private [atomic] type WriterMedicsMap = ConcurrentHashMap [TxId, Medic]

  private [atomic] def newTablesMap = new ConcurrentHashMap [TableId, TierTable]
  private [atomic] def newTableMedicsMap = new ConcurrentHashMap [TableId, TierMedic]
  private [atomic] def newWritersMap = new ConcurrentHashMap [TxId, WriteDeputy]
  private [atomic] def newWriterMedicsMap = new ConcurrentHashMap [TxId, Medic]

  private val locator = {
    import AtomicPicklers._
    tuple (tableId, bytes)
  }

  private [atomic] def resident (residents: Residents, table: TableId, key: Bytes): Boolean =
    residents.contains (locator, (table, key))

  private [atomic] def locate (atlas: Atlas, table: TableId, key: Bytes): Cohort =
    atlas.locate (locator, (table, key))

  private [atomic] def place (atlas: Atlas, table: TableId, key: Bytes): Int =
    atlas.place (locator, (table, key))
}
