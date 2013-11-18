package com.treode.store.local.temp

import scala.collection.JavaConversions._

import com.treode.store.TimedCell
import com.treode.store.local.TestableTimedTable

private class TestableTempTimedTable extends TempTimedTable with TestableTimedTable {

  def toSeq: Seq [TimedCell] =
    memtable.toSeq
}
