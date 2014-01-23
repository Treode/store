package com.treode.store.temp

import com.treode.store.TimedTableBehaviors
import org.scalatest.FreeSpec

class TempTimedTableSpec extends FreeSpec with TimedTableBehaviors {

  "The TempTable" - {
    behave like aTimedTable (new TestableTempKit (2))
  }}
