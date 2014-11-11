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
import scala.util.{Try => ScalaTry, Failure, Success}

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.{JavaType, JsonNode, ObjectMapper}
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.treode.async.Async
import com.treode.async.misc.{RichOption, parseInt, parseUnsignedLong}
import com.treode.cluster.{CellId, HostId}
import com.treode.finatra.BadRequestException
import com.treode.jackson.DefaultTreodeModule
import com.treode.store.{Bytes, Slice, TableId, TxClock, TxId, Value}
import com.twitter.app.Flaggable
import com.twitter.finagle.http.{MediaType, ParamMap}
import com.twitter.finatra.{Request, ResponseBuilder}
import com.twitter.util.{Future, Promise, Return, Throw, Try}
import org.jboss.netty.handler.codec.http.HttpResponseStatus

import Async.async

package object example {

  val textJson = new ObjectMapper with ScalaObjectMapper
  textJson.registerModule (DefaultScalaModule)
  textJson.registerModule (DefaultTreodeModule)
  textJson.registerModule (AppModule)

  val binaryJson = new ObjectMapper (new SmileFactory)

  val ContentType = "Content-Type"
  val ETag = "ETag"
  val IfModifiedSince = "If-Modified-Since"
  val IfUnmodifiedSince = "If-Unmodified-Since"
  val LastModificationBefore = "Last-Modification-Before"
  val TransactionId = "Transaction-ID"

  val Conflict = HttpResponseStatus.CONFLICT.getCode
  val NotFound = HttpResponseStatus.NOT_FOUND.getCode
  val NotModified = HttpResponseStatus.NOT_MODIFIED.getCode
  val Ok = HttpResponseStatus.OK.getCode
  val PreconditionFailed = HttpResponseStatus.PRECONDITION_FAILED.getCode

  implicit class RichBytes (bytes: Bytes) {

    def toJsonNode: JsonNode =
      binaryJson.readValue (bytes.bytes, classOf [JsonNode])
  }

  implicit class RichJsonNode (node: JsonNode) {

    def toBytes: Bytes =
      Bytes (binaryJson.writeValueAsBytes (node))
  }

  implicit class RichMutableMap [K, V] (map: mutable.Map [K, V]) {

    def getOrThrow (key: K, exc: Throwable): V =
      map.get (key) match {
        case Some (value) => value
        case None => throw exc
    }}

  implicit class RichParamMap (map: ParamMap) {

    def getOrThrow (key: String, exc: Throwable): String =
      map.get (key) match {
        case Some (value) => value
        case None => throw exc
    }}

  implicit class RichString (s: String) {

    def readJson(): JsonNode =
      textJson.readTree (s)
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

    def getTableId: TableId = {
       val s = request.routeParams.getOrThrow ("name", new BadRequestException ("Expected table ID"))
       parseUnsignedLong (s) .getOrThrow (new BadRequestException ("Bad table ID"))
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

    def readJson(): JsonNode =
      try {
        if (request.contentType != Some (MediaType.Json))
          throw new BadRequestException ("Expected JSON entity.")
        request.withReader (textJson.readTree _)
      } catch {
        case e: JsonProcessingException =>
          throw new BadRequestException (e.getMessage)
      }

    def readJsonAs [A] () (implicit m: Manifest [A]): A =
      try {
        request.withReader (textJson.readValue [A] (_))
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
    }}}
