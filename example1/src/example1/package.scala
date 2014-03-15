
import scala.collection.mutable
import scala.util.{Try => ScalaTry, Failure, Success}

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.treode.async.Async
import com.treode.store.{Bytes, TxClock}
import com.twitter.finagle.http.ParamMap
import com.twitter.finatra.Request
import com.twitter.util.{Future, Promise, Return, Throw, Try}

import Async.async

package example1 {

  class BadRequestException (val message: String) extends Exception
}

package object example1 {

  private val textJson = new ObjectMapper()
  private val binaryJson = new ObjectMapper (new SmileFactory)

  val ApplicationJson = "application/json"

  val ETag = "ETag"
  val IfMatch = "If-Match"

  val Conflict = 409
  val PreconditionFailed = 412

  implicit class RichAsync [A] (async: Async [A]) {

    def toTwitterFuture: Future [A] = {
      val promise = new Promise [A]
      async run {
        case Success (v) => promise.setValue (v)
        case Failure (t) => promise.setException (t)
      }
      promise
    }}

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

  implicit class RichRequest (request: Request) {

    def getIfMatch(): TxClock =
      request.headerMap.get (IfMatch) match {
        case Some (ct) =>
          TxClock
            .parse (ct)
            .getOrElse (throw new BadRequestException (s"Bad If-Match value: $ct"))
        case None =>
          TxClock.zero
      }

    def readJson(): JsonNode = {
      try {
        if (request.contentType != Some (ApplicationJson))
          throw new BadRequestException ("Expected JSON entity.")
        request.withReader (textJson.readTree _)
      } catch {
        case e: JsonParseException =>
          throw new BadRequestException (e.getMessage)
      }}}

  implicit class RichTry [A] (v: Try [A]) {

    def toAsync: Async [A] =
      async (_ (toScalaTry))

    def toTwitterFuture: Future [A] =
      Future.const (v)

    def toScalaTry: ScalaTry [A] =
      v match {
        case Return (v) => Success (v)
        case Throw (t) => Failure (t)
      }}}
