package example1

import scala.collection.JavaConversions._

import com.fasterxml.jackson.core._
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.treode.store.{Bytes, Cell}

object AppModule extends SimpleModule (
    "AppModule",
     new Version (0, 1, 0, "", "", ""),
     Map.empty [Class [_], JsonDeserializer [_]],
     List (BytesSerializer, CellSerializer))

object BytesSerializer extends StdSerializer [Bytes] (classOf [Bytes]) {

  def serialize (value: Bytes, jgen: JsonGenerator, provider: SerializerProvider) {
    jgen.writeObject (value.toJsonNode)
  }}

object CellSerializer extends StdSerializer [Cell] (classOf [Cell]) {

  def serialize (value: Cell, jgen: JsonGenerator, provider: SerializerProvider) {
    jgen.writeStartObject()
    jgen.writeObjectField ("key", value.key.string)
    jgen.writeObjectField ("time", value.time.time)
    value.value match {
      case Some (v) => jgen.writeObjectField ("value", v.toJsonNode)
      case None => jgen.writeNullField ("value")
    }
    jgen.writeEndObject()
  }}
