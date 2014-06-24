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

import java.io.DataOutput

private class SizingPickleContext extends PickleContext with DataOutput {

  private [this] var size = 0

  def result: Int = size

  // PickleContext
  def writeBytes (v: Array [Byte], offset: Int, length: Int) = size += length
  def writeByte (v: Byte) = size += 1
  def writeInt (v: Int) = size += 4
  def writeVarInt (v: Int) = size += 5
  def writeVarUInt (v: Int) = size += 5
  def writeShort (v: Short) = size += 2
  def writeLong (v: Long) = size += 8
  def writeVarLong (v: Long) = size += 9
  def writeVarULong (v: Long) = size += 9
  def writeFloat (v: Float) = size += 4
  def writeDouble (v: Double) = size += 8
  def writeString (v: String) = size += (5 + 2 * v.length)

  // DataOuput - PickleContext
  def write (v: Int) = size += 1
  def writeBoolean (v: Boolean) = size += 1
  def writeByte (v: Int) = size += 1
  def writeChar (v: Int) = size += 2
  def writeShort (v: Int) = size += 2
  def write (data: Array [Byte], offset: Int, length: Int) = size += length
  def write (data:  Array [Byte]) = size += data.length
  def writeBytes (v: String): Unit = ???
  def writeChars (v: String): Unit = ???
  def writeUTF (v: String): Unit = ???

  def toDataOutput: DataOutput = this
}
