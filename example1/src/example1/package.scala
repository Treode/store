
import scala.collection.mutable
import scala.util.{Try => ScalaTry, Failure, Success}

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.treode.async.Async
import com.treode.async.misc.parseUnsignedLong
import com.treode.cluster.{CellId, HostId}
import com.treode.store.{Bytes, TxClock}
import com.twitter.app.Flaggable
import com.twitter.finagle.http.{MediaType, ParamMap}
import com.twitter.finatra.Request
import com.twitter.util.{Future, Promise, Return, Throw, Try}
import org.jboss.netty.handler.codec.http.HttpResponseStatus

import Async.async

package example1 {

  class BadRequestException (val message: String) extends Exception
}

package object example1 {

  implicit val flaggableCellId: Flaggable [CellId] =
    Flaggable.mandatory (s => CellId (parseUnsignedLong (s) .get))

  implicit val flaggableHostId: Flaggable [HostId] =
    Flaggable.mandatory (s => HostId (parseUnsignedLong (s) .get))

  val textJson = new ObjectMapper()
  textJson.registerModule (TreodeModule)

  val binaryJson = new ObjectMapper (new SmileFactory)

  val ContentType = "Content-Type"
  val ETag = "ETag"
  val IfModifiedSince = "If-Modified-Since"
  val IfUnmodifiedSince = "If-Unmodified-Since"
  val LastModificationBefore = "Last-Modification-Before"

  val Conflict = HttpResponseStatus.CONFLICT.getCode
  val NotFound = HttpResponseStatus.NOT_FOUND.getCode
  val NotModified = HttpResponseStatus.NOT_MODIFIED.getCode
  val Ok = HttpResponseStatus.OK.getCode
  val PreconditionFailed = HttpResponseStatus.PRECONDITION_FAILED.getCode

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

    def readJson(): JsonNode = {
      try {
        if (request.contentType != Some (MediaType.Json))
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
