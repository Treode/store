package com.treode.store.simple

private class TestCell (val key: Int, val value: Option [Int]) {

  def this (cell: SimpleCell) = this (cell.key.int, cell.value map (_.int))
}
