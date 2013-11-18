package com.treode.store

private trait TestableLocalStore extends LocalStore {

  def expectCells (t: TableId) (cs: TimedCell*)
}
