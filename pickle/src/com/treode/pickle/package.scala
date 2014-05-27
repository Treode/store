package com.treode

package pickle {

  /** Superclass of all pickling and unpickling exceptions. */
  class PickleException (cause: Throwable) extends Exception (cause)

  class UnpicklingException (pickler: Pickler [_], cause: Throwable) extends Exception (cause) {
    override def getMessage = s"Unpickling $pickler failed: $cause"
  }

  class FrameBoundsException extends PickleException (null) {
    override def getMessage = "Object did not fit frame."
  }

  /** A tagged structure encountered an unknown tag. */
  class InvalidTagException (name: String, found: Long) extends PickleException (null) {
    override def getMessage = f"Invalid tag for $name, found $found%X"
  }}
