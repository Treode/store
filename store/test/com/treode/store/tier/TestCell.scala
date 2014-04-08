package com.treode.store.tier

import com.treode.store.Cell

private class TestCell (val key: Int, val value: Option [Int]) {

  def this (cell: Cell) = this (cell.key.int, cell.value map (_.int))
}
