package com.treode.store.local

import com.treode.async.CallbackCaptor
import com.treode.pickle.Picklers
import com.treode.store._
import org.scalatest.Assertions

import Assertions._

private trait TestableLocalKit extends TestableLocalStore {
  this: LocalKit =>

  def getTimedTable (id: TableId): TimedTable
}
