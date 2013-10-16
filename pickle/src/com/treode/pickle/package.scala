package com.treode

import com.esotericsoftware.kryo.io.{Input, Output}

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

  def pickle [A] (p: Pickler [A], v: A, o: Output) =
    p.p (v, new KryoPickleContext (o))

  def unpickle [A] (p: Pickler [A], i: Input): A =
    p.u (new KryoUnpickleContext (i))
}
