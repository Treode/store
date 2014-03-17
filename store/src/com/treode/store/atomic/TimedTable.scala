package com.treode.store.atomic

import com.treode.async.{Async, Scheduler}
import com.treode.disk.Disks
import com.treode.buffer.ArrayBuffer
import com.treode.store.{Bytes, StoreConfig, TableId, TimedCell, TxClock, Value}
import com.treode.store.tier.{TierCell, TierDescriptor, TierTable}

import TimedTable.{keyToBytes, cellToCell, cellToValue}

private class TimedTable (table: TierTable) {

  def get (key: Bytes, time: TxClock): Async [Value] = {
    val key1 = keyToBytes (key, time)
    val key2 = keyToBytes (key, 0)
    for (cell <- table.ceiling (key1, key2))
      yield cellToValue (cell)
  }

  def iterator: TimedIterator  =
    for (cell <- table.iterator)
      yield cellToCell (cell)

  def put (key: Bytes, time: TxClock, value: Bytes): Long =
    table.put (keyToBytes (key, time), value)

  def delete (key: Bytes, time: TxClock): Long =
    table.delete (keyToBytes (key, time))

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

  def keyToBytes (key: Bytes, time: TxClock): Bytes = {
    val buf = ArrayBuffer (key.length + 8)
    buf.writeBytes (key.bytes, 0, key.length)
    buf.writeLong (Long.MaxValue - time.time)
    Bytes (buf.data)
  }

  def cellToCell (cell: TierCell): TimedCell = {
    val buf = ArrayBuffer (cell.key.bytes)
    val key = new Array [Byte] (cell.key.length - 8)
    buf.readBytes (key, 0, cell.key.length - 8)
    val time = buf.readLong()
    TimedCell (Bytes (key), Long.MaxValue - time, cell.value)
  }

  def cellToValue (cell: TierCell): Value = {
    val buf = ArrayBuffer (cell.key.bytes)
    buf.readPos = cell.key.length - 8
    val time = buf.readLong()
    Value (Long.MaxValue - time, cell.value)
  }

  def apply (id: TableId) (implicit scheduler: Scheduler, disks: Disks, config: StoreConfig): TimedTable =
    new TimedTable (TierTable (table, id.id))
}
