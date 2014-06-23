package com.treode.jackson

import java.nio.file.{Path, Paths}
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser, JsonToken}
import com.fasterxml.jackson.databind.{DeserializationContext, JsonNode, SerializerProvider}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer

object PathSerializer extends StdSerializer [Path] (classOf [Path]) {

  def serialize (value: Path, jgen: JsonGenerator, provider: SerializerProvider) {
    jgen.writeString (value.toString)
  }}

object PathDeserializer extends StdDeserializer [Path] (classOf [Path]) {

  def deserialize (jparser: JsonParser, context: DeserializationContext): Path = {
    if (jparser.getCurrentToken != JsonToken.VALUE_STRING)
      throw context.mappingException ("Malformed path.")
    Paths.get (jparser.getValueAsString)
  }}
