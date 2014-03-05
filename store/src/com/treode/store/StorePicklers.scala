package com.treode.store

import com.treode.cluster.{HostId, PortId}
import com.treode.disk.Position
import com.treode.pickle.Picklers
import com.treode.store.paxos.BallotNumber
import com.treode.store.tier.TierTable

private trait StorePicklers extends Picklers {

  def ballotNumber = BallotNumber.pickler
  def bytes = Bytes.pickler
  def catId = CatalogId.pickler
  def cohort = Cohort.pickler
  def hostId = HostId.pickler
  def portId = PortId.pickler
  def pos = Position.pickler
  def readOp = ReadOp.pickler
  def tableId = TableId.pickler
  def tierMeta = TierTable.Meta.pickler
  def txClock = TxClock.pickler
  def txId = TxId.pickler
  def value = Value.pickler
  def writeOp = WriteOp.pickler
}

private object StorePicklers extends StorePicklers
