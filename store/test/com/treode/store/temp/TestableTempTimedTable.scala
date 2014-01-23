package com.treode.store.temp

import scala.collection.JavaConversions._

import com.treode.store.{TimedCell, TimedTable}

private class TestableTempTimedTable extends TempTimedTable with TimedTable {

  def toSeq: Seq [TimedCell] =
    memtable.toSeq
}
