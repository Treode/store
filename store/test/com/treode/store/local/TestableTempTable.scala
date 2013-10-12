package com.treode.store.local

import scala.collection.JavaConversions._

import com.treode.store.tier.Cell

private class TestableTempTable extends TempTable {

  def toSeq: Seq [Cell] =
    memtable.toSeq
}
