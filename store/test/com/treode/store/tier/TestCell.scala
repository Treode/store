package com.treode.store.tier

private class TestCell (val key: Int, val value: Option [Int]) {

  def this (cell: TierCell) = this (cell.key.int, cell.value map (_.int))
}
