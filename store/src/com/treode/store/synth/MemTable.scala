package com.treode.store.synth

import java.util.concurrent.ConcurrentSkipListSet

import com.treode.store.{Bytes, Cell, TxClock}

private class MemTable {

  private val cells = new ConcurrentSkipListSet [Cell] (Cell)
  private var _byteSize = 0

  def byteSize = _byteSize

  def read (key: Bytes, time: TxClock): Option [Cell] = {
    val cell = cells.floor (Cell (key, time, None))
    if (cell == null) None else Some (cell)
  }

  def commit (wt: TxClock, key: Bytes, value: Option [Bytes]) {
    val cell = Cell (key, wt, value)
    cells.add (cell)
    synchronized {
      _byteSize += cell.byteSize
    }}}
