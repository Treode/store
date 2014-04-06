package com.treode.store.tier

import com.treode.disk.{Disks, PageDescriptor, PageHandler, TypeId}
import com.treode.store.StorePicklers
import com.treode.pickle.Pickler

class TierDescriptor [K, V] private (
    val id: TypeId,
    val pkey: Pickler [K],
    val pval: Pickler [V]
) {

  private [tier] val pager = {
    import StorePicklers._
    PageDescriptor (id, ulong, TierPage.pickler)
  }

  def handle (handler: PageHandler [Long]) (implicit launch: Disks.Launch): Unit =
    pager.handle (handler)

  override def toString = s"TierDescriptor($id)"
}

object TierDescriptor {

  def apply [K, V] (id: TypeId, pkey: Pickler [K], pval: Pickler [V]): TierDescriptor [K, V] =
    new TierDescriptor (id, pkey, pval)
}
