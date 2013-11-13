package com.treode.store

import com.treode.cluster.HostId
import com.treode.pickle.Picklers

private class StorePicklers extends Picklers {

  def bytes = Bytes.pickle
  def hostId = HostId.pickle
  def readBatch = ReadBatch.pickle
  def readOp = ReadOp.pickle
  def tableId = TableId.pickle
  def txClock = TxClock.pickle
  def txId = TxId.pickle
  def value = Value.pickle
  def writeBatch = WriteBatch.pickle
  def writeOp = WriteOp.pickle
}

private object StorePicklers extends StorePicklers
