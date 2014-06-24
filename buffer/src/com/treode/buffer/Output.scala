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

trait Output {

  def writeBytes (data: Array [Byte], offset: Int, length: Int)
  def writeByte (v: Byte)
  def writeShort (v: Short)
  def writeInt (v: Int)
  def writeVarUInt (v: Int)
  def writeVarInt (v: Int)
  def writeLong (v: Long)
  def writeVarULong (v: Long)
  def writeVarLong (v: Long)
  def writeFloat (v: Float)
  def writeDouble (v: Double)
  def writeString (v: String)
}

object Output {

  def asDataOutput (out: Output): DataOutput =
    new DataOutputWrapper (out)
}
