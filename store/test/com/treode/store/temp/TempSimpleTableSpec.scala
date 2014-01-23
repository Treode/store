package com.treode.store.temp

import java.nio.file.Paths
import scala.util.Random

import com.treode.store.SimpleTableBehaviors
import org.scalatest.FreeSpec

class TempSimpleTableSpec extends FreeSpec with SimpleTableBehaviors {

  private val store = new TestableTempKit (2)

  "The TempSimpleTable" - {
    behave like aSimpleTable (store)
  }}
