package example1

import com.treode.async.Async
import com.treode.async.misc.{RichOption, parseLong}
import com.treode.cluster.HostId
import com.treode.store._
import com.twitter.finatra.Request
import org.joda.time.Instant

import Async.{guard, supply}
import WriteOp._

class Resource (host: HostId, store: Store) extends AsyncFinatraController {

  private def parseWrite (request: Request): Async [(TxId, TxClock, Seq [WriteOp])] =
    supply {
      val tx = request.getTransactionId (host)
      val ct = request.getIfUnmodifiedSince
      val _table = request.routeParams.getOrThrow ("name", new BadRequestException ("Expected table ID"))
      val table = parseLong (_table) .getOrThrow (new BadRequestException ("Bad table ID"))
      val key = request.params.getOrThrow ("key", new BadRequestException ("Expected key"))
      val value = request.readJson()
      (tx, ct, Seq (Update (table, Bytes (key), value.toBytes)))
    }

  private def parseRead (request: Request): Async [(TxClock, TxClock, Seq [ReadOp])] =
    supply {
      val rt = request.getLastModificationBefore
      val ct = request.getIfModifiedSince
      val _table = request.routeParams.getOrThrow ("name", new BadRequestException ("Expected table ID"))
      val table = parseLong (_table) .getOrThrow (new BadRequestException ("Bad table ID"))
      val key = request.params.getOrThrow ("key", new BadRequestException ("Expected key"))
      (rt, ct, Seq (ReadOp (table, Bytes (key))))
    }

  get ("/table/:name") { request =>
    for {
      (rt, ct, ops) <- parseRead (request)
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

  put ("/table/:name") { request =>
    guard {
      for {
        (tx, ct, ops) <- parseWrite (request)
        vt <- store.write (tx, ct, ops:_*)
      } yield {
        render.ok.header (ETag, vt.toString) .nothing
      }
    } .recover {
      case _: CollisionException =>
        render.status (Conflict) .nothing
      case _: StaleException =>
        render.status (PreconditionFailed) .nothing
    }}}
