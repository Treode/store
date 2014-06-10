package example1

import scala.collection.JavaConversions._
import com.fasterxml.jackson.core._
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.treode.cluster.HostId
import com.treode.store.Cohort

import Cohort.{Empty, Issuing, Moving, Settled}

object TreodeModule extends SimpleModule (
    "TreodeModule",
     new Version (0, 1, 0, "", "", ""),
     Map [Class [_], JsonDeserializer [_]] (
         classOf [Cohort] -> CohortDeserializer,
         classOf [HostId] -> HostIdDeserializer),
     List (
         EmptySerializer,
         HostIdSerializer,
         IssuingSerializer,
         MovingSerializer,
         SettledSerializer))

object EmptySerializer
extends StdSerializer [Empty.type] (Empty.getClass.asInstanceOf [Class [Empty.type]]) {

  def serialize (value: Empty.type, jgen: JsonGenerator, provider: SerializerProvider) {
    jgen.writeStartObject()
    jgen.writeObjectField ("state", "empty")
    jgen.writeEndObject()
  }}

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
          try {
            if (s.toLowerCase.startsWith ("host:"))
              HostId (java.lang.Long.parseLong (s.substring (5), 16))
            else
              HostId (java.lang.Long.decode (s))
          } catch {
            case _: NumberFormatException =>
              throw context.weirdStringException (s, classOf [HostId], "Malformed host ID")
          }
        case _ =>
          throw context.wrongTokenException (jparser, JsonToken.VALUE_STRING, "Malformed host ID")
      }}}

object IssuingSerializer extends StdSerializer [Issuing] (classOf [Issuing]) {

  def serialize (value: Issuing, jgen: JsonGenerator, provider: SerializerProvider) {
    val codec = jgen.getCodec
    jgen.writeStartObject()
    jgen.writeObjectField ("state", "issuing")
    jgen.writeArrayFieldStart("origin")
    for (h <- value.origin)
      codec.writeValue (jgen, h)
    jgen.writeEndArray()
    jgen.writeArrayFieldStart("target")
    for (h <- value.target)
      codec.writeValue (jgen, h)
    jgen.writeEndArray()
    jgen.writeEndObject()
  }}

object MovingSerializer extends StdSerializer [Moving] (classOf [Moving]) {

  def serialize (value: Moving, jgen: JsonGenerator, provider: SerializerProvider) {
    val codec = jgen.getCodec
    jgen.writeStartObject()
    jgen.writeObjectField ("state", "moving")
    jgen.writeArrayFieldStart("origin")
    for (h <- value.origin)
      codec.writeValue (jgen, h)
    jgen.writeEndArray()
    jgen.writeArrayFieldStart("target")
    for (h <- value.target)
      codec.writeValue (jgen, h)
    jgen.writeEndArray()
    jgen.writeEndObject()
  }}

object SettledSerializer extends StdSerializer [Settled] (classOf [Settled]) {

  def serialize (value: Settled, jgen: JsonGenerator, provider: SerializerProvider) {
    val codec = jgen.getCodec
    jgen.writeStartObject()
    jgen.writeObjectField ("state", "settled")
    jgen.writeArrayFieldStart("hosts")
    for (h <- value.hosts)
      codec.writeValue (jgen, h)
    jgen.writeEndArray()
    jgen.writeEndObject()
  }}

object CohortDeserializer extends StdDeserializer [Cohort] (classOf [Cohort]) {

  def readState (node: TreeNode, context: DeserializationContext): String = {
    val state = node.get ("state")
    if (state == null)
      "settled"
    else if (!state.isValueNode)
      throw context.mappingException ("Malformed state")
    else
      state.asInstanceOf [JsonNode] .asText.toLowerCase
  }

  def readHosts (jparser: JsonParser, node: TreeNode, name: String): Set [HostId] = {
    val values = node.get (name)
    if (values == null)
      Set.empty
    else
      jparser.getCodec.treeToValue (values, classOf [Array [HostId]]) .toSet
  }

  def deserialize (jparser: JsonParser, context: DeserializationContext): Cohort = {
    val codec = jparser.getCodec
    val node = codec.readTree [TreeNode] (jparser)
    val state = readState (node, context)
    val hosts = readHosts (jparser, node, "hosts")
    val origin = readHosts (jparser, node, "origin")
    val target = readHosts (jparser, node, "target")
    state match {
      case "empty" if hosts.isEmpty && origin.isEmpty && target.isEmpty =>
        Cohort.empty
      case "empty" =>
        throw context.mappingException ("An empty cohort should not have hosts, origin or target")
      case "settled" if !hosts.isEmpty && origin.isEmpty && target.isEmpty =>
        Cohort.Settled (hosts)
      case "settled" =>
        throw context.mappingException ("A settled cohort should have only hosts")
      case "issuing" if hosts.isEmpty && !origin.isEmpty && !target.isEmpty =>
        Cohort.Issuing (origin, target)
      case "issuing" =>
        throw context.mappingException ("An issuing cohort should have only an origin and target")
      case "moving" if hosts.isEmpty && !origin.isEmpty && !target.isEmpty =>
        Cohort.Moving (origin, target)
      case "moving" =>
        throw context.mappingException ("A moving cohort should have only an origin and target")
      case _ =>
        throw context.mappingException ("Malformed cohort")
    }}}
