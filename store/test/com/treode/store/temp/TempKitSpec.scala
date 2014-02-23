package com.treode.store.temp

import com.treode.store.{LocalStoreBehaviors, StoreConfig}
import org.scalatest.FreeSpec

class TempKitSpec extends FreeSpec with LocalStoreBehaviors {

  "The TempKit should" - {
    implicit val config = StoreConfig (4, 1<<16)
    behave like aLocalStore (_ => new TestableTempKit)
  }}
