package com.treode.store.local

trait TestableTimedTable extends TimedTable {

  def toSeq: Seq [TimedCell]
}
