package com.treode.store

import scala.util.Random

import com.treode.async.Callback
import com.treode.store._
import org.scalatest.FreeSpec

import Cardinals.One
import Fruits.Apple
import SimpleTestTools._

trait SimpleTableBehaviors {
  this: FreeSpec =>

  private def nextTable = TableId (Random.nextLong)

  def aSimpleTable (s: SimpleStore) = {

    "A SimpleTable should get, put and delete" in {
      val t = s.openSimpleTable (nextTable)
      t.put (Apple, One, Callback.ignore)
      t.getAndExpect (Apple, Some (One))
      t.del (Apple, Callback.ignore)
      t.getAndExpect (Apple, None)
      t.close()
    }}}
