package com.treode.async

package io {

  class FrameBoundsException extends Exception {
    override def getMessage = "Object did not fit frame."
  }

  class FrameNotRecognizedException (id: Any) extends Exception {
    override def getMessage = s"$id not recognzied."
  }}
