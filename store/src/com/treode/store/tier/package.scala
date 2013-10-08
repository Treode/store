package com.treode.store

import com.treode.cluster.concurrent.Callback
import com.treode.pickle.{Pickler, Picklers, PickleContext, UnpickleContext}

package tier {

  private trait Entry {
    def key: Bytes
    def time: TxClock
    def byteSize: Int
  }

  private trait Block {
    def get (i: Int): Entry
    def find (key: Bytes, time: TxClock): Int
    def size: Int
    def isEmpty: Boolean
    def last: Entry
  }

  private trait BlockCache {
    def get (pos: Long, cb: Callback [Block])
  }

  private trait BlockWriter {
    def maxBlockSize: Int
    def write (block: Block, cb: Callback [Long])
  }}

package object tier {
  import Picklers.unsignedInt

  // Determine length of common prefix.
  private def common (x: Array [Byte], y: Array [Byte]): Int = {
    var i = 0
    while (i < x.length && i < y.length && x (i) == y (i))
      i += 1
    i
  }

  // Write first key; write full byte array.
  private [tier] def writeKey (key: Bytes, ctx: PickleContext) {
    val _key = key.bytes
    unsignedInt.p (_key.length, ctx)
    ctx.writeBytes (_key, 0, _key.size)
  }

  // Read first key; read full byte array.
  private [tier] def readKey (ctx: UnpickleContext): Bytes = {
    val length = unsignedInt.u (ctx)
    val bytes = new Array [Byte] (length)
    ctx.readBytes (bytes, 0, length)
    Bytes (bytes)
  }

  // Write subsequent key; skip common prefix.
  private [tier] def writeKey (prev: Bytes, key: Bytes, ctx: PickleContext) {
    val _prev = prev.bytes
    val _key = key.bytes
    val prefix = common (_prev, _key)
    unsignedInt.p (_key.length, ctx)
    unsignedInt.p (prefix, ctx)
    assert (prefix <= _prev.length && prefix <= _key.length)
    if (prefix != _prev.length)
      ctx.writeBytes (_key, prefix, _key.length - prefix)
  }

  // Read subsequent key; use common prefix from previous key.
  private [tier] def readKey (prev: Bytes, ctx: UnpickleContext): Bytes = {
    val _prev = prev.bytes
    val length = unsignedInt.u (ctx)
    val prefix = unsignedInt.u (ctx)
    assert (prefix <= _prev.length && prefix <= length)
    if (prefix != _prev.length) {
      val bytes = new Array [Byte] (length)
      System.arraycopy (_prev, 0, bytes, 0, prefix)
      ctx.readBytes (bytes, prefix, length - prefix)
      Bytes (bytes)
    } else {
      prev
    }}}
