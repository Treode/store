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

trait Input {

  def readBytes (data: Array [Byte], offset: Int, length: Int)
  def readByte(): Byte
  def readShort(): Short
  def readInt(): Int
  def readVarUInt(): Int
  def readVarInt(): Int
  def readLong(): Long
  def readVarULong(): Long
  def readVarLong(): Long
  def readFloat(): Float
  def readDouble(): Double
  def readString(): String
}

object Input {

  def asDataInput (in: Input): DataInput =
    new DataInputWrapper (in)
}
