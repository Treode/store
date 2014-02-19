package com.treode

import java.io.Closeable
import java.util.logging.{Level, Logger}

import com.treode.async.Callback
import com.treode.pickle.Pickler
import com.treode.store.locks.LockSet

import Level.WARNING

package store {

  trait ReadCallback extends Callback [Seq [Value]]

  trait WriteCallback extends Callback [TxClock] {
    def collisions (ks: Set [Int])
    def advance()
  }

  trait Store {
    def read (rt: TxClock, ops: Seq [ReadOp], cb: ReadCallback)
    def write (xid: TxId, ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback)
  }

  private trait PrepareCallback extends Callback [Preparation] {
    def collisions (ks: Set [Int])
    def advance()
  }

  private trait PreparableStore {
    def read (rt: TxClock, ops: Seq [ReadOp], cb: ReadCallback)
    def prepare (ct: TxClock, ops: Seq [WriteOp], cb: PrepareCallback)
    def commit (wt: TxClock, ops: Seq [WriteOp], cb: Callback [Unit])
  }

  private trait LocalStore extends PreparableStore with Closeable
}
