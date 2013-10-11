package com.treode.store

import com.treode.cluster.concurrent.Callback

package object local {

  private [local] implicit class RichBytes (v: Bytes) {
    def ## (time: Int) = Cell (v, TxClock (time), None)
    def :: (cell: Cell): Cell = Cell (cell.key, cell.time, Some (v))
    def :: (time: Int): Value = Value (time, Some (v))
  }

  private [local] implicit class RichOption (v: Option [Bytes]) {
    def :: (time: Int): Value = Value (time, v)
  }

  private [local] def toSeq (iter: CellIterator): Seq [Cell] = {
    val builder = Seq.newBuilder [Cell]
    val loop = new Callback [Cell] {
      def apply (cell: Cell) {
        builder += cell
        if (iter.hasNext)
          iter.next (this)
      }
      def fail (t: Throwable) = throw t
    }
    if (iter.hasNext)
      iter.next (loop)
    builder.result
  }}
