package com.treode.store.atomic

import com.treode.async.{Async, Scheduler}
import com.treode.disk.Disks
import com.treode.buffer.ArrayBuffer
import com.treode.store.{Bytes, Cell, CellIterator, StoreConfig, TableId, TxClock, Value}
import com.treode.store.tier.{TierDescriptor, TierTable}

private class TimedTable (table: TierTable) {

  def get (key: Bytes, time: TxClock): Async [Value] = {
    for (cell <- table.ceiling (key, time))
      yield Value (cell.time, cell.value)
  }

  def iterator: CellIterator  =
    table.iterator

  def put (key: Bytes, time: TxClock, value: Bytes): Long =
    table.put (key, time, value)

  def delete (key: Bytes, time: TxClock): Long =
    table.delete (key, time)

  def probe (groups: Set [Long]): Set [Long] =
    table.probe (groups)

  def compact (groups: Set [Long]): Async [TierTable.Meta] =
    table.compact (groups)

  def checkpoint(): Async [TierTable.Meta] =
    table.checkpoint()
}

private object TimedTable {

  val table = {
    import com.treode.store.StorePicklers._
    TierDescriptor (0xB500D51FACAEA961L, unit, unit)
  }

  def apply (id: TableId) (implicit scheduler: Scheduler, disks: Disks, config: StoreConfig): TimedTable =
    new TimedTable (TierTable (table, id.id))
}
