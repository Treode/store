package example1

import scala.util.Random

import com.treode.async.Async
import com.treode.async.misc.{RichOption, parseLong}
import com.treode.store.{Bytes, ReadOp, Store, TxClock, TxId, WriteOp, WriteResult}
import com.twitter.finatra.Request

import Async.supply
import WriteOp._
import WriteResult._

class Resource (store: Store) extends AsyncFinatraController {

  private var tx = 0L

  private def nextTx = {
    tx += 1
    TxId (tx)
  }

  private def parseRead (request: Request): Async [Seq [ReadOp]] =
    supply {
      val _table = request.routeParams.getOrThrow ("name", new BadRequestException ("Expected table ID"))
      val table = parseLong (_table) .getOrThrow (new BadRequestException ("Bad table ID"))
      val key = request.params.getOrThrow ("key", new BadRequestException ("Expected key"))
      Seq (ReadOp (table, Bytes (key)))
    }

  private def parseWrite (request: Request): Async [(TxClock, Seq [WriteOp])] =
    supply {
      val ct = request.getIfMatch()
      val _table = request.routeParams.getOrThrow ("name", new BadRequestException ("Expected table ID"))
      val table = parseLong (_table) .getOrThrow (new BadRequestException ("Bad table ID"))
      val key = request.params.getOrThrow ("key", new BadRequestException ("Expected key"))
      val value = request.readJson()
      (ct, Seq (Update (table, Bytes (key), value.toBytes)))
    }

  get ("/table/:name") { request =>
    for {
      ops <- parseRead (request)
      vs <- store.read (TxClock.now, ops)
    } yield {
      val v = vs.head
      v.value match {
        case Some (value) =>
          render.header (ETag, v.time.toString) .json (value.toJsonNode)
        case None =>
          render.notFound.nothing
      }}}

  put ("/table/:name") { request =>
    for {
      (ct, ops) <- parseWrite (request)
      result <- store.write (nextTx, ct, ops)
    } yield {
      result match {
        case Written (vt) =>
          render.ok.header (ETag, vt.toString) .nothing
        case Collided (ks: Seq [Int]) =>
          render.status (Conflict) .nothing
        case Stale =>
          render.status (PreconditionFailed) .nothing
      }}}}
