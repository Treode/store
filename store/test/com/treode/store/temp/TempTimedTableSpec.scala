package com.treode.store.temp

import com.treode.store.{StoreConfig, TimedTableBehaviors}
import org.scalatest.FreeSpec

class TempTimedTableSpec extends FreeSpec with TimedTableBehaviors {

  "The TempTable" - {
    implicit val config = StoreConfig (4, 1<<16)
    behave like aTimedTable (new TestableTempKit)
  }}
