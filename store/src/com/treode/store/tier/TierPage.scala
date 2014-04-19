package com.treode.store.tier

import com.treode.store.StorePicklers

private trait TierPage

private object TierPage {

  val pickler = {
    import StorePicklers._
    tagged [TierPage] (
        0x1 -> IndexPage.pickler,
        0x2 -> TierCellPage.pickler,
        0x3 -> BloomFilter.pickler)
  }}
