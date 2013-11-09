package com.treode

package pickle {

  /** Superclass of all pickling and unpickling exceptions. */
  class PickleException extends Exception

  class BufferUnderflowException (required: Int, available: Int) extends Exception {
    override def getMessage = s"Buffer underflow, $required required, $available available."
  }

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

  def pickle [A] (p: Pickler [A], v: A, b: Buffer) =
    p.p (v, new BufferPickleContext (b))

  def unpickle [A] (p: Pickler [A], b: Buffer): A =
    p.u (new BufferUnpickleContext (b))

  def toByteArray [A] (p: Pickler [A], v: A): Array [Byte] = {
    val buf = Buffer (12)
    com.treode.pickle.pickle (p, v, buf)
    val bytes = new Array [Byte] (buf.readableBytes)
    buf.readBytes (bytes, 0, bytes.length)
    bytes
  }

  def fromByteArray [A] (p: Pickler [A], bytes: Array [Byte]): A = {
    val buf = Buffer (12)
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
