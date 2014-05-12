package com.treode.store.atomic

import com.treode.store.StorePicklers

private class AtomicPicklers extends StorePicklers {

  def writeResponse = WriteResponse.pickler
}

private object AtomicPicklers extends AtomicPicklers
