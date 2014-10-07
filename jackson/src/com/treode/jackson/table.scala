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

package com.treode.jackson

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.treode.store.TableDigest

import TableDigest.{Tier => TierDigest}

object TableDigestSerializer extends StdSerializer [TableDigest] (classOf [TableDigest]) {

  def serialize (value: TableDigest, jgen: JsonGenerator, provider: SerializerProvider) {
    val codec = jgen.getCodec
    jgen.writeStartObject()
    jgen.writeObjectField ("id", f"0x${value.id.id}%016X")
    jgen.writeFieldName ("tiers")
    jgen.writeStartArray()
    for (t <- value.tiers)
      codec.writeValue (jgen, t)
    jgen.writeEndArray()
    jgen.writeEndObject()
  }}

object TierDigestSerializer extends StdSerializer [TierDigest] (classOf [TierDigest]) {

  def serialize (value: TierDigest, jgen: JsonGenerator, provider: SerializerProvider) {
    val codec = jgen.getCodec
    jgen.writeStartObject()
    jgen.writeObjectField ("keys", value.keys)
    jgen.writeObjectField ("entries", value.entries)
    jgen.writeObjectField ("earliest", value.earliest.time)
    jgen.writeObjectField ("latest", value.latest.time)
    jgen.writeObjectField ("diskBytes", value.diskBytes)
    jgen.writeEndObject()
  }}
