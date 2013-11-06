package com.treode.store.local

import scala.collection.JavaConversions._

private class TestableTempTimedTable extends TempTimedTable {

  def toSeq: Seq [TimedCell] =
    memtable.toSeq
}
