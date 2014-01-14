package com.treode.async

package io {

  class FrameNotRecognizedException (id: Any) extends Exception {
    override def getMessage = s"$id not recognzied."
  }

  class FrameOverflowException extends Exception

  class FrameUnderflowException extends Exception
}
