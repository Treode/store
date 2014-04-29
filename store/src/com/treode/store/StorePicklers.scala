package com.treode.store

import com.treode.cluster.{HostId, PortId}
import com.treode.disk.{ObjectId, Position, TypeId}
import com.treode.pickle.Picklers
import com.treode.store.paxos.BallotNumber
import com.treode.store.tier.TierTable
import org.joda.time.Instant

private trait StorePicklers extends Picklers {

  lazy val instant = wrap (ulong) build (new Instant (_)) inspect (_.getMillis)

  def atlas = Atlas.pickler
  def ballotNumber = BallotNumber.pickler
  def bytes = Bytes.pickler
  def catId = CatalogId.pickler
  def cell = Cell.pickler
  def cohort = Cohort.pickler
  def hostId = HostId.pickler
  def key = Key.pickler
  def portId = PortId.pickler
  def pos = Position.pickler
  def readOp = ReadOp.pickler
  def residents = Residents.pickler
  def tableId = TableId.pickler
  def tierMeta = TierTable.Meta.pickler
  def txClock = TxClock.pickler
  def txId = TxId.pickler
  def txStatus = TxStatus.pickler
  def value = Value.pickler
  def writeOp = WriteOp.pickler
}

private object StorePicklers extends StorePicklers
