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

import java.io.DataInput

private class DataInputWrapper (in: Input) extends DataInput {

  def readBoolean(): Boolean = in.readByte() != 0
  def readByte(): Byte = in.readByte()
  def readChar(): Char = in.readShort().toChar
  def readDouble(): Double = in.readDouble()
  def readFloat(): Float = in.readFloat()
  def readInt(): Int = in.readInt()
  def readLong(): Long = in.readLong()
  def readShort(): Short = in.readShort()
  def readUnsignedByte(): Int = in.readByte().toInt & 0xFF
  def readUnsignedShort(): Int = in.readShort().toInt & 0xFFFF

  def readFully (data: Array [Byte], offset: Int, length: Int): Unit =
    in.readBytes (data, offset, length)

  def readFully (data: Array [Byte]): Unit =
    in.readBytes (data, 0, data.length)

  def skipBytes (length: Int): Int = ???
  def readLine(): String = ???
  def readUTF(): String = ???
}
