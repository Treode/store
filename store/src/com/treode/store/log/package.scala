package com.treode.store

import com.treode.cluster.concurrent.Callback
import com.treode.pickle.{Pickler, Picklers, PickleContext, UnpickleContext}

package log {

  private [store] trait Block

  private [store] trait BlockCache {

    def get (pos: Long, cb: Callback [Block])
  }

  private [store] trait BlockWriter {

    def maxBlockSize: Int
    def write (block: Block, cb: Callback [Long])
  }}
