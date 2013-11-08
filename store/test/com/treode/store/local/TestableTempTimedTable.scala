package com.treode.store.local

import scala.collection.JavaConversions._

private class TestableTempTimedTable extends TempTimedTable with TestableTimedTable {

  def toSeq: Seq [TimedCell] =
    memtable.toSeq
}
