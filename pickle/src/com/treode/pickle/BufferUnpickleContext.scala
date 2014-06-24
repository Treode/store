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

package com.treode.pickle

import java.io.DataInput
import com.treode.buffer.Input

private class BufferUnpickleContext (in: Input) extends UnpickleContext {

  def readBytes (data: Array [Byte], offset: Int, length: Int) =
    in.readBytes (data, offset, length)

  def readByte() = in.readByte()
  def readShort() = in.readShort()
  def readInt() = in.readInt()
  def readVarInt() = in.readVarInt()
  def readVarUInt() = in.readVarUInt()
  def readLong() = in.readLong()
  def readVarLong() = in.readVarLong()
  def readVarULong() = in.readVarULong()
  def readFloat() = in.readFloat()
  def readDouble() = in.readDouble()
  def readString() = in.readString()

  def toDataInput = Input.asDataInput (in)
}
