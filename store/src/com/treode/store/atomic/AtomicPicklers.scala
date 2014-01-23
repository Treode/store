package com.treode.store.atomic

import com.treode.store.StorePicklers

private class AtomicPicklers extends StorePicklers {

  def txStatus = TxStatus.pickle
  def readResponse = ReadResponse.pickle
  def writeResponse = WriteResponse.pickle
}

private object AtomicPicklers extends AtomicPicklers
