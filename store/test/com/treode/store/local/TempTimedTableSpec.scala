package com.treode.store.local

import org.scalatest.FreeSpec

class TempTimedTableSpec extends FreeSpec with TimedTableBehaviors {

  "The TempTable" - {
    behave like aTimedTable (new TestableTempLocalStore (4))
  }}
