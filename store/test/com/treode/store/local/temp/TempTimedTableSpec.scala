package com.treode.store.local.temp

import com.treode.store.local.TimedTableBehaviors
import org.scalatest.FreeSpec

class TempTimedTableSpec extends FreeSpec with TimedTableBehaviors {

  "The TempTable" - {
    behave like aTimedTable (new TestableTempKit (2))
  }}
