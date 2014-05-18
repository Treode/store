package com.treode.store.tier

import com.treode.disk.{Disk, PageDescriptor, PageHandler, TypeId}
import com.treode.store.{Cell, Residents, StorePicklers, TableId}
import com.treode.pickle.Pickler

private [store] class TierDescriptor private (
    val id: TypeId
) (
    val residency: (Residents, TableId, Cell) => Boolean
) {

  private [tier] val pager = {
    import StorePicklers._
    PageDescriptor (id, ulong, TierPage.pickler)
  }

  def handle (handler: PageHandler [Long]) (implicit launch: Disk.Launch): Unit =
    pager.handle (handler)

  override def toString = s"TierDescriptor($id)"
}

private [store] object TierDescriptor {

  def apply (id: TypeId) (residency: (Residents, TableId, Cell) => Boolean): TierDescriptor =
    new TierDescriptor (id) (residency)
}
