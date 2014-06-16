package example1

import com.fasterxml.jackson.databind.JsonNode
import com.treode.async.Async
import com.treode.async.misc.{RichOption, parseLong}
import com.treode.cluster.HostId
import com.treode.store._
import com.twitter.finatra.{Request, ResponseBuilder}
import org.joda.time.Instant

import Async.{guard, supply}
import WriteOp._

class Resource (host: HostId, store: Store) extends AsyncFinatraController {

  def read (request: Request, table: TableId, key: String): Async [ResponseBuilder] = {
    val rt = request.getLastModificationBefore
    val ct = request.getIfModifiedSince
    val _table = request.routeParams.getOrThrow ("name", new BadRequestException ("Expected table ID"))
    val table = parseLong (_table) .getOrThrow (new BadRequestException ("Bad table ID"))
    val ops = Seq (ReadOp (table, Bytes (key)))
    for {
      vs <- store.read (rt, ops:_*)
    } yield {
      val v = vs.head
      v.value match {
        case Some (value) if ct < v.time =>
          render.header (ETag, v.time.toString) .json (value.toJsonNode)
        case Some (value) =>
          render.status (NotModified) .nothing
        case None =>
          render.notFound.nothing
      }}}

  def scan (request: Request, table: TableId): Async [ResponseBuilder] = {
    val rt = request.getLastModificationBefore
    val ct = request.getIfModifiedSince
    val window = Window.Recent (rt, true, ct, false)
    val iter = store
        .scan (table, Bound.firstKey, window, Slice.all)
        .filter (_.value.isDefined)
        .map (_.value.get.toJsonNode)
    for {
      vs <- iter.toSeq
    } yield {
      render.json (vs)
    }}

  get ("/table/:name") { request =>
    val _table = request.routeParams.getOrThrow ("name", new BadRequestException ("Expected table ID"))
    val table = parseLong (_table) .getOrThrow (new BadRequestException ("Bad table ID"))
    val key = request.params.get ("key")
    if (key.isDefined)
      read (request, table, key.get)
    else
      scan (request, table)
  }

  get ("/history/:name") { request =>
    val _table = request.routeParams.getOrThrow ("name", new BadRequestException ("Expected table ID"))
    val table = parseLong (_table) .getOrThrow (new BadRequestException ("Bad table ID"))
    val start = Bound.Inclusive (Key.MinValue)
    val rt = request.getLastModificationBefore
    val ct = request.getIfModifiedSince
    val window = Window.Between (rt, true, ct, false)
    val iter = store
        .scan (table, Bound.firstKey, window, Slice.all)
        .map (_.timedValue.toJsonNode)
    for {
      vs <- iter.toSeq
    } yield {
      render.json (vs)
    }}

  put ("/table/:name") { request =>
    val tx = request.getTransactionId (host)
    val ct = request.getIfUnmodifiedSince
    val _table = request.routeParams.getOrThrow ("name", new BadRequestException ("Expected table ID"))
    val table = parseLong (_table) .getOrThrow (new BadRequestException ("Bad table ID"))
    val key = request.params.getOrThrow ("key", new BadRequestException ("Expected key"))
    val value = request.readJson()
    val ops = Seq (Update (table, Bytes (key), value.toBytes))
    (for {
      vt <- store.write (tx, ct, ops:_*)
    } yield {
      render.ok.header (ETag, vt.toString) .nothing
    }) .recover {
      case _: StaleException =>
        render.status (PreconditionFailed) .nothing
    }}

  delete ("/table/:name") { request =>
    val tx = request.getTransactionId (host)
    val ct = request.getIfUnmodifiedSince
    val _table = request.routeParams.getOrThrow ("name", new BadRequestException ("Expected table ID"))
    val table = parseLong (_table) .getOrThrow (new BadRequestException ("Bad table ID"))
    val key = request.params.getOrThrow ("key", new BadRequestException ("Expected key"))
    val ops = Seq (Delete (table, Bytes (key)))
    (for {
      vt <- store.write (tx, ct, ops:_*)
    } yield {
      render.ok.header (ETag, vt.toString) .nothing
    }) .recover {
      case _: StaleException =>
        render.status (PreconditionFailed) .nothing
    }}}
