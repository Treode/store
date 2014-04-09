package com.treode.store.tier

import com.treode.disk.{Disks, PageDescriptor, PageHandler, TypeId}
import com.treode.store.StorePicklers
import com.treode.pickle.Pickler

class TierDescriptor private (val id: TypeId) {

  private [tier] val pager = {
    import StorePicklers._
    PageDescriptor (id, ulong, TierPage.pickler)
  }

  def handle (handler: PageHandler [Long]) (implicit launch: Disks.Launch): Unit =
    pager.handle (handler)

  override def toString = s"TierDescriptor($id)"
}

object TierDescriptor {

  def apply (id: TypeId): TierDescriptor =
    new TierDescriptor (id)
}
