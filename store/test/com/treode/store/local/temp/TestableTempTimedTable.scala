package com.treode.store.local.temp

import scala.collection.JavaConversions._

import com.treode.store.local.{TestableTimedTable, TimedCell}

private class TestableTempTimedTable extends TempTimedTable with TestableTimedTable {

  def toSeq: Seq [TimedCell] =
    memtable.toSeq
}
