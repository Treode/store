package example1

import com.fasterxml.jackson.databind.JsonNode
import com.treode.async.Async
import com.treode.disk.ControllerException
import com.treode.store.TimeoutException
import com.twitter.finatra.{Controller => FinatraController, Request, ResponseBuilder}
import com.twitter.util.Future

import Async.guard

trait AsyncFinatraController {

  val delegate = new FinatraController

  def render = delegate.render

  def renderJson [A] (v: A): ResponseBuilder =
    render.json (textJson.convertValue (v, classOf [JsonNode]))

  private def adapt (cb: Request => Async [ResponseBuilder]) (request: Request): Future [ResponseBuilder] =
    guard {
      cb (request)
    } .recover {
      case e: BadRequestException =>
        render.status (400) .plain (e.message + "\n")
      case e: ControllerException =>
        render.status (400) .plain (e.getMessage + "\n")
      case e: TimeoutException =>
        render.status (500) .plain ("Server timed out.\n")
    } .toTwitterFuture

  def head (path: String) (callback: Request => Async [ResponseBuilder]): Unit =
    delegate.head (path) (adapt (callback))

  def options (path: String) (callback: Request => Async [ResponseBuilder]): Unit =
    delegate.options (path) (adapt (callback))

  def get (path: String) (callback: Request => Async [ResponseBuilder]): Unit =
    delegate.get (path) (adapt (callback))

  def post (path: String) (callback: Request => Async [ResponseBuilder]): Unit =
    delegate.post (path) (adapt (callback))

  def put (path: String) (callback: Request => Async [ResponseBuilder]): Unit =
    delegate.put (path) (adapt (callback))

  def patch (path: String) (callback: Request => Async [ResponseBuilder]): Unit =
    delegate.patch (path) (adapt (callback))

  def delete (path: String) (callback: Request => Async [ResponseBuilder]): Unit =
    delegate.delete (path) (adapt (callback))
}
