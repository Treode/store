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
    try {
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
      }}}
