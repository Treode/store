package example1

import com.treode.async.Async
import com.twitter.finatra.{Controller => FinatraController, Request, ResponseBuilder}
import com.twitter.util.Future

trait AsyncFinatraController {

  val delegate = new FinatraController

  def render = delegate.render

  private def adapt (cb: Request => Async [ResponseBuilder]) (request: Request): Future [ResponseBuilder] = {
    cb (request) .toTwitterFuture rescue {
      case e: BadRequestException =>
        render.status (400) .plain (e.message) .toFuture
    }}

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
