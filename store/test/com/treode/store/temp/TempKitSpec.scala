package com.treode.store.temp

import com.treode.store.LocalStoreBehaviors
import org.scalatest.FreeSpec

class TempKitSpec extends FreeSpec with LocalStoreBehaviors {

  "The TempKit should" - {
    behave like aLocalStore (new TestableTempKit (2))
    behave like aMultithreadableStore (100, new Adaptor (new TestableTempKit (4)))
  }}
