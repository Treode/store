package com.treode.store.local.temp

import scala.collection.JavaConversions._

import com.treode.store.TimedCell
import com.treode.store.local.TimedTable

private class TestableTempTimedTable extends TempTimedTable with TimedTable {

  def toSeq: Seq [TimedCell] =
    memtable.toSeq
}
