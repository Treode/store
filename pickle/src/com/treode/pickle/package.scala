package com.treode

import com.treode.buffer.{PagedBuffer, Input, Output}

package pickle {

  /** Superclass of all pickling and unpickling exceptions. */
  class PickleException extends Exception

  /** A tagged structure encountered an unknown tag. */
  class InvalidTagException (name: String, found: Long) extends PickleException {
    override def getMessage = "Invalid tag for " + name + ", found " + found
  }

  /** How to read and write an object of a particular type. */
  trait Pickler [A] {
    def p (v: A, ctx: PickleContext)
    def u (ctx: UnpickleContext): A
  }}

package object pickle {

  def pickle [A] (p: Pickler [A], v: A, b: Output) =
    p.p (v, new BufferPickleContext (b))

  def unpickle [A] (p: Pickler [A], b: Input): A =
    p.u (new BufferUnpickleContext (b))

  def toByteArray [A] (p: Pickler [A], v: A): Array [Byte] = {
    val buf = PagedBuffer (12)
    com.treode.pickle.pickle (p, v, buf)
    val bytes = new Array [Byte] (buf.readableBytes)
    buf.readBytes (bytes, 0, bytes.length)
    bytes
  }

  def fromByteArray [A] (p: Pickler [A], bytes: Array [Byte]): A = {
    val buf = PagedBuffer (12)
    buf.writeBytes (bytes, 0, bytes.length)
    val v = com.treode.pickle.unpickle (p, buf)
    require (buf.readableBytes == 0, "Bytes remain after unpickling.")
    v
  }

  def size [A] (p: Pickler [A], v: A): Int = {
    val sizer = new SizingPickleContext
    p.p (v, sizer)
    sizer.result
  }}
