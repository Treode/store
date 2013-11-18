package com.treode.store.cluster.atomic

import com.treode.store.StorePicklers

private class AtomicPicklers extends StorePicklers {

  def atomicResponse = AtomicResponse.pickle
  def atomicStatus = AtomicStatus.pickle
  def deputyStatus = DeputyStatus.pickle
}

private object AtomicPicklers extends AtomicPicklers
