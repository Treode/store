package com.treode.pickle

import com.treode.buffer.{Input, PagedBuffer, Output}

/** How to read and write an object of a particular type. */
trait Pickler [A] {

  def p (v: A, ctx: PickleContext)

  def u (ctx: UnpickleContext): A

  def pickle (v: A, b: Output): Unit =
    p (v, new BufferPickleContext (b))

  def unpickle (b: Input): A =
    u (new BufferUnpickleContext (b))

  def byteSize (v: A): Int = {
    val sizer = new SizingPickleContext
    p (v, sizer)
    sizer.result
  }

  def toByteArray (v: A): Array [Byte] = {
    val buf = PagedBuffer (12)
    pickle (v, buf)
    val bytes = new Array [Byte] (buf.readableBytes)
    buf.readBytes (bytes, 0, bytes.length)
    bytes
  }

  def fromByteArray (bytes: Array [Byte]): A = {
    val buf = PagedBuffer (12)
    buf.writeBytes (bytes, 0, bytes.length)
    val v = unpickle (buf)
    require (buf.readableBytes == 0, "Bytes remain after unpickling.")
    v
  }}
