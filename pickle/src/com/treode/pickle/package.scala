package com.treode

package pickle {

  /** Superclass of all pickling and unpickling exceptions. */
  class PickleException extends Exception

  class FrameBoundsException extends PickleException {
    override def getMessage = "Object did not fit frame."
  }

  /** A tagged structure encountered an unknown tag. */
  class InvalidTagException (name: String, found: Long) extends PickleException {
    override def getMessage = f"Invalid tag for $name, found $found%X"
  }}
