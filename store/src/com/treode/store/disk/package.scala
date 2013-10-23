package com.treode.store

import com.treode.cluster.concurrent.Callback

package disk {

  private [store] trait Block

  private [store] trait DiskSystem {

    def maxBlockSize: Int

    def read (pos: Long, cb: Callback [Block])

    def write (block: Block, cb: Callback [Long])
  }}
