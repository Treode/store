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

import java.lang.Integer.highestOneBit
import scala.reflect.ClassTag

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.treode.async.misc.{RichOption, parseInt, parseUnsignedLong}
import com.treode.cluster.HostId
import com.treode.store.{Bytes, TableId, TxClock, TxId, Slice}
import com.treode.twitter.finagle.http.BadRequestException
import io.netty.handler.codec.http.HttpResponse
import unfiltered.netty.ReceivedMessage
import unfiltered.request.{Body, HttpRequest}
import unfiltered.response._
import org.joda.time.format.DateTimeFormat

package object example {

  val textJson = new ObjectMapper with ScalaObjectMapper
  textJson.registerModule (DefaultScalaModule)
  textJson.registerModule (AppModule)

  val binaryJson = new ObjectMapper (new SmileFactory)

  val httpDate = DateTimeFormat.forPattern ("EEE, d MMM HH:mm:ss y zzz")

  type Request = HttpRequest [ReceivedMessage]

  type Response = ResponseFunction [HttpResponse]

  object ValueTxClock extends HeaderName ("Value-TxClock")

  object ReadTxClock extends HeaderName ("Read-TxClock")

  implicit class RichAny (v: Any) {

    def toJsonText: String =
      textJson.writeValueAsString (v)
  }

  implicit class RichBytes (bytes: Bytes) {

    def toJsonNode: JsonNode =
      binaryJson.readValue (bytes.bytes, classOf [JsonNode])
  }

  implicit class RichJsonNode (node: JsonNode) {

    def toBytes: Bytes =
      Bytes (binaryJson.writeValueAsBytes (node))
  }

  implicit class RichRequest (request: Request) {

    def header (name: String): Option [String] = {
      val hs = request.headers (name)
      if (hs.hasNext) Some (hs.next) else None
    }

    def optIntParam (name: String): Option [Int] =
      request.parameterValues (name)
      .headOption
      .map { value =>
        parseInt (value) .getOrThrow (new BadRequestException (s"Bad integer for $name: $value"))
      }

    def optTxClockHeader (name: String): Option [TxClock] =
      header (name) .map { value =>
        TxClock.parse (value) .getOrThrow (new BadRequestException (s"Bad time for $name: $value"))
      }

    def conditionTxClock (default: TxClock): TxClock =
      optTxClockHeader ("Condition-TxClock") getOrElse (default)

    def requestTxClock: TxClock =
      optTxClockHeader ("Request-TxClock") getOrElse (TxClock.now)

    def getSlice: Slice = {
      val slice = optIntParam ("slice")
      val nslices = optIntParam ("nslices")
      if (slice.isDefined && nslices.isDefined) {
        if (nslices.get < 1 || highestOneBit (nslices.get) != nslices.get)
          throw new BadRequestException ("Number of slices must be a power of two and at least one.")
        if (slice.get < 0 || nslices.get <= slice.get)
          throw new BadRequestException ("The slice must be between 0 (inclusive) and the number of slices (exclusive).")
        Slice (slice.get, nslices.get)
      } else if (slice.isDefined || nslices.isDefined) {
        throw new BadRequestException ("Both slice and nslices are needed together")
      } else {
        Slice.all
      }}

    def getTransactionId (host: HostId): TxId =
      header ("Transaction-ID") match {
        case Some (tx) =>
          TxId
            .parse (tx)
            .getOrElse (throw new BadRequestException (s"Bad Transaction-ID value: $tx"))
        case None =>
          TxId.random (host)
      }

    def readJson [A: Manifest]: A =
      try {
        textJson.readValue [A] (Body.reader (request))
      } catch {
        case e: JsonProcessingException =>
          throw new BadRequestException (e.getMessage)
      }}

  implicit class RichString (s: String) {

    def getTableId: TableId =
      TableId (
        parseUnsignedLong (s)
        .getOrThrow (new BadRequestException (s"Bad table ID: $s")))

    def fromJson [A: Manifest]: A =
      textJson.readValue [A] (s)
  }}
