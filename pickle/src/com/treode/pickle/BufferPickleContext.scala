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

import com.treode.buffer.Output

private class BufferPickleContext (out: Output) extends PickleContext {

  def writeBytes (v: Array [Byte], offset: Int, length: Int) =
    out.writeBytes (v, offset, length)

  def writeByte (v: Byte) = out.writeByte (v)
  def writeInt (v: Int) = out.writeInt (v)
  def writeVarInt (v: Int) = out.writeVarInt (v)
  def writeVarUInt (v: Int) = out.writeVarUInt (v)
  def writeShort (v: Short) = out.writeShort (v)
  def writeLong (v: Long) = out.writeLong (v)
  def writeVarLong (v: Long) = out.writeVarLong (v)
  def writeVarULong (v: Long) = out.writeVarULong (v)
  def writeFloat (v: Float) = out.writeFloat (v)
  def writeDouble (v: Double) = out.writeDouble (v)
  def writeString (v: String) = out.writeString (v)

  def toDataOutput = Output.asDataOutput (out)
}
