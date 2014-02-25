package com.treode.store.atomic

import com.treode.store.StorePicklers

private class AtomicPicklers extends StorePicklers {

  def activeStatus = WriteDeputy.ActiveStatus.pickler
  def timedStoreMeta = TimedStore.Meta.pickler
  def txStatus = TxStatus.pickler
  def readResponse = ReadResponse.pickler
  def writeResponse = WriteResponse.pickler
}

private object AtomicPicklers extends AtomicPicklers
