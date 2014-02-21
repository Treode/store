package com.treode.store.temp

import com.treode.store.{LocalStoreBehaviors, StoreConfig}
import org.scalatest.FreeSpec

class TempKitSpec extends FreeSpec with LocalStoreBehaviors {

  "The TempKit should" - {
    implicit val config = StoreConfig (4, 1<<16)
    behave like aLocalStore (new TestableTempKit)
    behave like aMultithreadableStore (100, new Adaptor (new TestableTempKit))
  }}
