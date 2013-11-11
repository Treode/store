package com.treode.store.local.temp

import java.nio.file.Paths
import scala.util.Random

import com.treode.store.local.{SimpleTableBehaviors, TestableLocalStore}
import org.scalatest.{BeforeAndAfterAll, FreeSpec}

class TempSimpleTableSpec extends FreeSpec with SimpleTableBehaviors {

  private val store = new TempSimpleStore

  "The TempSimpleTable" - {
    behave like aSimpleTable (store)
  }}
