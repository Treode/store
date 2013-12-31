package com.treode.buffer

/**
  * See https://issues.scala-lang.org/browse/SI-7934
  */
@deprecated ("", "")
private object DeprecationCoral {

  def getBytes (src: String, srcBegin: Int, srcEnd: Int, dst: Array [Byte], dstBegin: Int) =
    src.getBytes (srcBegin, srcEnd, dst, dstBegin)
}
