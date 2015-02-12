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
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.treode.async.BatchIterator
import com.treode.async.misc
import com.treode.cluster.HostId
import com.treode.jackson.DefaultTreodeModule
import com.treode.store.{Bytes, TxClock}
import com.treode.store.alt.Froster
import com.treode.twitter.finagle.http, http.{BadRequestException, RichResponse}
import com.twitter.finagle.http.{MediaType, Request, Response, Status}
import com.twitter.finagle.http.filter.{CommonLogFormatter, LoggingFilter}
import com.twitter.logging.Logger
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.jboss.netty.handler.codec.http.HttpResponseStatus

package object movies {

  implicit val textJson = new ObjectMapper with ScalaObjectMapper
  textJson.registerModule (DefaultScalaModule)
  textJson.registerModule (DefaultTreodeModule)
  textJson.registerModule (new JodaModule)

  // See http://www.lorrin.org/blog/2013/06/28/custom-joda-time-dateformatter-in-jackson/
  textJson.registerModule (new SimpleModule {

    addSerializer (classOf [DateTime], new StdSerializer [DateTime] (classOf [DateTime]) {

      def serialize (value: DateTime,
        gen: JsonGenerator, provider: SerializerProvider): Unit =
        gen.writeString (ISODateTimeFormat.date().print(value))
    })
  })

  val binaryJson = new ObjectMapper (new SmileFactory) with ScalaObjectMapper
  binaryJson.registerModule (new DefaultScalaModule)
  binaryJson.registerModule (new JodaModule)

  val prettyJson = textJson.writerWithDefaultPrettyPrinter

  object respond {

    def apply (req: Request, status: HttpResponseStatus = Status.Ok): Response = {
      val rsp = req.response
      rsp.status = status
      rsp
    }

    def ok (req: Request, time: TxClock): Response = {
      val rsp = req.response
      rsp.status = Status.Ok
      rsp.valueTxClock = time
      rsp
    }

    def created (req: Request, time: TxClock, location: String): Response = {
      val rsp = req.response
      rsp.status = Status.Created
      rsp.valueTxClock = time
      rsp.location = location
      rsp
    }

    def json (req: Request, value: Any): Response  = {
      implicit val mapper = if (req.pretty) prettyJson else textJson
      val rsp = req.response
      rsp.status = Status.Ok
      rsp.json = value
      rsp
    }

    def json (req: Request, time: TxClock, value: Any): Response  = {
      implicit val mapper = if (req.pretty) prettyJson else textJson
      val rsp = req.response
      rsp.status = Status.Ok
      rsp.date = req.requestTxClock
      rsp.lastModified = time
      rsp.readTxClock = req.requestTxClock
      rsp.valueTxClock = time
      rsp.vary = "Request-TxClock"
      rsp.json = value
      rsp
    }

    def json [A] (req: Request, iter: BatchIterator [A]): Response  = {
      implicit val mapper = if (req.pretty) prettyJson else textJson
      val rsp = req.response
      rsp.status = Status.Ok
      rsp.json = iter
      rsp
    }}

  object LoggingFilter extends LoggingFilter (Logger ("access"), new CommonLogFormatter)

  def toBase36 (v: Long): String =
    java.lang.Long.toString (v, 36)

  implicit class RichAny [A] (value: A) {

    def orBadRequest (message: String): Unit =
      if (value == null) throw new BadRequestException (message)

    def orDefault (default: A): A =
      if (value == null) default else value
  }

  implicit class RichBoolean (value: Boolean) extends misc.RichBoolean (value) {

    def orBadRequest (message: String): Unit =
      orThrow (new BadRequestException (message))
  }

  implicit class RichOption [A] (value: Option [A]) extends misc.RichOption (value) {

    def getOrBadRequest (message: String): A =
      getOrThrow (new BadRequestException (message))
  }

  implicit class RichFrosterCompanion (obj: Froster.type) {

    def bson [A] (implicit m: Manifest [A]): Froster [A] =
      new Froster [A] {
        private val c = m.runtimeClass.asInstanceOf [Class [A]]
        def freeze (v: A): Bytes = Bytes (binaryJson.writeValueAsBytes (v))
        def thaw (v: Bytes): A = binaryJson.readValue (v.bytes, c)
      }}

  implicit class RichRandom (random: Random) {

    def nextId(): String =
      toBase36 (random.nextLong & 0xFFFFFFFFFFL)
  }

  implicit class RichRequest (request: Request) extends http.RichRequest (request) {

    def id (prefix: String): String =
      request.path.substring (prefix.length)

    def pretty: Boolean =
      // If the accept header lacks "application/json", then pretty print the response.
      request.headerMap.get ("Accept") match {
        case Some (accept) => !(accept contains MediaType.Json)
        case None => true
      }

    def query: String =
      request.params
        .get ("q")
        .getOrThrow (new BadRequestException ("Query parameter q is required."))
  }

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
