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

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser, JsonToken}
import com.fasterxml.jackson.databind.{DeserializationContext, JsonNode, SerializerProvider}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.treode.async.misc.parseUnsignedLong
import com.treode.cluster.HostId

object HostIdSerializer extends StdSerializer [HostId] (classOf [HostId]) {

  def serialize (value: HostId, jgen: JsonGenerator, provider: SerializerProvider) {
    jgen.writeString (f"0x${value.id}%016X")
  }}

object HostIdDeserializer extends StdDeserializer [HostId] (classOf [HostId]) {

  def deserialize (jparser: JsonParser, context: DeserializationContext): HostId =
    jparser.getCurrentToken match {
      case JsonToken.VALUE_NUMBER_INT =>
        HostId (jparser.getValueAsLong)
      case JsonToken.VALUE_STRING =>
        val s = jparser.getValueAsString
        val id = parseUnsignedLong (s)
        if (id.isEmpty)
          throw context.weirdStringException (s, classOf [HostId], "Malformed host ID")
        HostId (id.get)
      case _ =>
        throw context.wrongTokenException (jparser, JsonToken.VALUE_STRING, "Malformed host ID")
    }}
