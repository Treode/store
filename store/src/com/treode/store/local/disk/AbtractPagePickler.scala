package com.treode.store.local.disk

import com.treode.pickle.{PickleContext, Pickler, Picklers, UnpickleContext}
import com.treode.store.Bytes

abstract class AbstractPagePickler [T, E] extends Pickler [T] {

  private [this] val count = Picklers.uint

  /** Determine length of common prefix. */
  private def common (x: Array [Byte], y: Array [Byte]): Int = {
    var i = 0
    while (i < x.length && i < y.length && x (i) == y (i))
      i += 1
    i
  }

  /** Write first key; write full byte array. */
  protected def writeKey (key: Bytes, ctx: PickleContext) {
    val _key = key.bytes
    count.p (_key.length, ctx)
    ctx.writeBytes (_key, 0, _key.size)
  }

  /** Read first key; read full byte array. */
  protected def readKey (ctx: UnpickleContext): Bytes = {
    val length = count.u (ctx)
    val bytes = new Array [Byte] (length)
    ctx.readBytes (bytes, 0, length)
    Bytes (bytes)
  }

  /** Write subsequent key; skip common prefix. */
  protected def writeKey (prev: Bytes, key: Bytes, ctx: PickleContext) {
    val _prev = prev.bytes
    val _key = key.bytes
    val prefix = common (_prev, _key)
    count.p (_key.length, ctx)
    count.p (prefix, ctx)
    assert (prefix <= _prev.length && prefix <= _key.length)
    if (prefix != _prev.length)
      ctx.writeBytes (_key, prefix, _key.length - prefix)
  }

  /** Read subsequent key; use common prefix from previous key. */
  protected def readKey (prev: Bytes, ctx: UnpickleContext): Bytes = {
    val _prev = prev.bytes
    val length = count.u (ctx)
    val prefix = count.u (ctx)
    assert (prefix <= _prev.length && prefix <= length)
    if (prefix != _prev.length) {
      val bytes = new Array [Byte] (length)
      System.arraycopy (_prev, 0, bytes, 0, prefix)
      ctx.readBytes (bytes, prefix, length - prefix)
      Bytes (bytes)
    } else {
      prev
    }}

  protected def _p (entries: Array [E], ctx: PickleContext) {
    count.p (entries.size, ctx)
    if (entries.size > 0) {
      var prev = entries (0)
      writeEntry (prev, ctx)
      var i = 1
      while (i < entries.size) {
        val next = entries (i)
        writeEntry (prev, next, ctx)
        prev = next
        i += 1
      }}}

  protected def _u (ctx: UnpickleContext) (implicit m: Manifest [E]): Array [E] = {
    val size = count.u (ctx)
    val entries = new Array [E] (size)
    if (size > 0) {
      var prev = readEntry (ctx)
      entries (0) = prev
      var i = 1
      while (i < size) {
        prev = readEntry (prev, ctx)
        entries (i) = prev
        i += 1
      }}
    entries
  }

  /** Write first entry; write full key. */
  protected def writeEntry (entry: E, ctx: PickleContext)

  /** Read first entry; read full byte array. */
  protected def readEntry (ctx: UnpickleContext): E

  /** Write subsequent entry; skip common prefix of previous key. */
  protected def writeEntry (prev: E, entry: E, ctx: PickleContext)

  /** Read subsequent entry, use common prefix from previous key. */
  protected def readEntry (prev: E, ctx: UnpickleContext): E

}
