package com.treode.store.atomic

import com.treode.buffer.ArrayBuffer
import com.treode.store.Bytes

/** A TimedKey converts into a Bytes object that (a) preserves the ordering of the embedded byte
  * key and (b) reverses the ordering of time.  This sorts properly in a TierTable.
  */
private class TimedKey (val key: Bytes, val time: Long) {

  def toBytes: Bytes = {
    val buf = ArrayBuffer (key.length + 8)
    buf.writeBytes (key.bytes, 0, key.length)
    buf.writeLong (Long.MaxValue - time)
    Bytes (buf.data)
  }}

private object TimedKey {

  def apply (key: Bytes, time: Long): TimedKey =
    new TimedKey (key, time)

  def apply (bytes: Bytes): TimedKey = {
    val buf = ArrayBuffer (bytes.bytes)
    val key = new Array [Byte] (bytes.length - 8)
    buf.readBytes (key, 0, bytes.length - 8)
    val time = buf.readLong()
    new TimedKey (Bytes (key), time)
  }}
