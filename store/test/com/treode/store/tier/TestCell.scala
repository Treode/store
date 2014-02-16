package com.treode.store.tier

private class TestCell (val key: Int, val value: Option [Int]) {

  def this (cell: Cell) = this (cell.key.int, cell.value map (_.int))
}
