/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode

package buffer {

  trait Buffer extends InputBuffer with OutputBuffer

  class BufferOverflowException (required: Int, available: Int) extends Exception {
    override def getMessage = s"Buffer overflow, $required required, $available available."
  }

  class BufferUnderflowException (required: Int, available: Int) extends Exception {
    override def getMessage = s"Buffer underflow, $required required, $available available."
  }}

package object buffer {

  private [buffer] def isAscii (v: String): Boolean = {
    var i = 0
    while (i < v.length) {
      if (v.charAt(i) > 127)
        return false
      i += 1
    }
    return true
  }

  private [buffer] def twopow (n: Int): Int = {
    var x = n
    x |= x >> 1
    x |= x >> 2
    x |= x >> 4
    x |= x >> 8
    x |= x >> 16
    x + 1
  }}
