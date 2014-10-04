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

import scala.collection.mutable
import scala.util.Random

import com.fasterxml.jackson.core.{JsonGenerator, JsonProcessingException}
import com.fasterxml.jackson.databind.{ObjectMapper, SerializerProvider}
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.fasterxml.jackson.datatype.joda.JodaModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.treode.finatra.BadRequestException
import com.treode.async.misc.parseUnsignedLong
import com.treode.cluster.{CellId, HostId}
import com.treode.store.{Bytes, Slice, TxClock, TxId}
import com.treode.store.alt.Froster
import com.twitter.app.Flaggable
import com.twitter.finagle.http.MediaType
import com.twitter.finatra.{Request, ResponseBuilder}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.jboss.netty.handler.codec.http.HttpResponseStatus

package object movies {

  private val binaryJson = new ObjectMapper (new SmileFactory)
  binaryJson.registerModule (new DefaultScalaModule)
  binaryJson.registerModule (new JodaModule)

  private val textJson = new ObjectMapper
  textJson.registerModule (DefaultScalaModule)
  textJson.registerModule (new JodaModule)

  // See http://www.lorrin.org/blog/2013/06/28/custom-joda-time-dateformatter-in-jackson/
  textJson.registerModule (new SimpleModule {

    addSerializer (classOf [DateTime], new StdSerializer [DateTime] (classOf [DateTime]) {

      def serialize (value: DateTime, 
        gen: JsonGenerator, provider: SerializerProvider): Unit =
        gen.writeString (ISODateTimeFormat.date().print(value))
    })
  })

  implicit val flaggableCellId: Flaggable [CellId] =
    Flaggable.mandatory (s => CellId (parseUnsignedLong (s) .get))

  implicit val flaggableHostId: Flaggable [HostId] =
    Flaggable.mandatory (s => HostId (parseUnsignedLong (s) .get))

  val ContentType = "Content-Type"
  val ETag = "ETag"
  val IfModifiedSince = "If-Modified-Since"
  val IfUnmodifiedSince = "If-Unmodified-Since"
  val Location = "Location"
  val LastModificationBefore = "Last-Modification-Before"
  val TransactionId = "Transaction-ID"

  val BadRequest = HttpResponseStatus.BAD_REQUEST.getCode
  val NotFound = HttpResponseStatus.NOT_FOUND.getCode
  val NotImplemented = HttpResponseStatus.NOT_IMPLEMENTED.getCode
  val NotModified = HttpResponseStatus.NOT_MODIFIED.getCode
  val Ok = HttpResponseStatus.OK.getCode
  val PreconditionFailed = HttpResponseStatus.PRECONDITION_FAILED.getCode

  def toBase36 (v: Long): String =
    java.lang.Long.toString (v, 36)

  implicit class RichAny [A] (value: A) {

    def orBadRequest (message: String): Unit =
      if (value == null) throw new BadRequestException (message)

    def orDefault (default: A): A =
      if (value == null) default else value

    def toJson: String =
      textJson.writeValueAsString (value)
  }

  implicit class RichFrosterCompanion (obj: Froster.type) {

    def bson [A] (implicit m: Manifest [A]): Froster [A] =
      new Froster [A] {
        private val c = m.runtimeClass.asInstanceOf [Class [A]]
        def freeze (v: A): Bytes = Bytes (binaryJson.writeValueAsBytes (v))
        def thaw (v: Bytes): A = binaryJson.readValue (v.bytes, c)
      }}

  implicit class RichMutableMap [K, V] (map: mutable.Map [K, V]) {

    def getOrBadRequest (key: K, message: String): V =
      map.get (key) getOrBadRequest (message)
  }

  implicit class RichOption [A] (value: Option [A]) {

    def getOrBadRequest (message: String): A =
      value match {
        case Some (v) => v
        case None => throw new BadRequestException (message)
      }}

  implicit class RichRandom (random: Random) {

    def nextId(): String =
      toBase36 (random.nextLong & 0xFFFFFFFFFFL)
  }

  implicit class RichRequest (request: Request) {

    def getIfModifiedSince: TxClock =
      request.headerMap.get (IfModifiedSince) match {
        case Some (ct) =>
          TxClock
            .parse (ct)
            .getOrElse (throw new BadRequestException (s"Bad If-Modified-Since value: $ct"))
        case None =>
          TxClock.MinValue
      }

    def getIfUnmodifiedSince: TxClock =
      request.headerMap.get (IfUnmodifiedSince) match {
        case Some (ct) =>
          TxClock
            .parse (ct)
            .getOrElse (throw new BadRequestException (s"Bad If-Unmodified-Since value: $ct"))
        case None =>
          TxClock.now
      }

    def getId: Option [String] =
      request.routeParams.get ("id")

    def getLastModificationBefore: TxClock =
      request.headerMap.get (LastModificationBefore) match {
        case Some (rt) =>
          TxClock
            .parse (rt)
            .getOrElse (throw new BadRequestException (s"Bad Last-Modification-Before value: $rt"))
        case None =>
          TxClock.now
      }

    def getSlice: Slice = {
      val slice = request.params.getInt ("slice")
      val nslices = request.params.getInt ("nslices")
      if (slice.isDefined && nslices.isDefined)
        Slice (slice.get, nslices.get)
      else if (slice.isDefined || nslices.isDefined)
        throw new BadRequestException ("Both slice and nslices are needed together")
      else
        Slice.all
    }

    def getTransactionId (host: HostId): TxId =
      request.headerMap.get (TransactionId) match {
        case Some (tx) =>
          TxId
            .parse (tx)
            .getOrElse (throw new BadRequestException (s"Bad Transaction-ID value: $tx"))
        case None =>
          TxId.random (host)
      }

    def readJsonAs [A] () (implicit m: Manifest [A]): A =
      try {
        request.withReader (textJson.readValue (_, m.runtimeClass.asInstanceOf [Class [A]]))
      } catch {
        case e: JsonProcessingException =>
          throw new BadRequestException (e.getMessage)
      }}

  implicit class RichResponseBuilder (response: ResponseBuilder) {

    // It would be handy if we could add our modules to the mapper Finatra's using.
    def appjson (v: Any): ResponseBuilder = {
      response.contentType (MediaType.Json)
      response.body (textJson.writeValueAsBytes (v))
      response
    }}

  implicit class RichSeq [A] (vs: Seq [A]) {

    def merge (p: A => Boolean, v1: A): Option [Seq [A]] =
      vs.find (p) match {
        case Some (v0) if v0 != v1 => Some (v1 +: vs.filterNot (p))
        case Some (_) => None
        case None => Some (v1 +: vs)
      }

    def remove (p: A => Boolean): Option [Seq [A]] =
      vs.find (p) match {
        case Some (v0) => Some (vs.filterNot (p))
        case None => None
      }}

  implicit class RichString (s: String) {

    def fromJson [A] (implicit m: Manifest [A]): A =
      textJson.readValue (s, m.runtimeClass.asInstanceOf [Class [A]])
  }}
