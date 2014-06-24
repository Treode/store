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

package com.treode.buffer

import java.io.DataOutput

private class DataOutputWrapper (out: Output) extends DataOutput {

  def write (v: Int): Unit = out.writeByte (v.toByte)
  def writeBoolean (v: Boolean): Unit = out.writeByte (if (v) 1 else 0)
  def writeByte (v: Int): Unit = out.writeByte (v.toByte)
  def writeChar (v: Int): Unit = out.writeShort (v.toShort)
  def writeDouble (v: Double): Unit = out.writeDouble (v)
  def writeFloat (v: Float): Unit = out.writeFloat (v)
  def writeInt (v: Int): Unit = out.writeInt (v)
  def writeLong (v: Long): Unit = out.writeLong (v)
  def writeShort (v: Int): Unit = out.writeShort (v.toShort)

  def write (data: Array [Byte], offset: Int, length: Int): Unit =
    out.writeBytes (data, offset, length)

  def write (data:  Array [Byte]): Unit =
    out.writeBytes (data, 0, data.length)

  def writeBytes (v: String): Unit = ???
  def writeChars (v: String): Unit = ???
  def writeUTF (v: String): Unit = ???
}
