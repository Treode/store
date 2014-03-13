package example1


import com.twitter.finatra.{Controller, ResponseBuilder}
import com.twitter.util.Future

object Example1 extends Controller {

  def rescue (response: Future [ResponseBuilder]): Future [ResponseBuilder] = {
    response rescue {
      case e: BadRequestException =>
        render.status (400) .plain (e.message) .toFuture
    }}

  post ("/hello/:name") { request => rescue {
    for {
      body <- request .readJson() .toFuture
    } yield {
      val name = request.routeParams.getOrElse ("name", "sexy")
      render.plain (s"Hello $name")
    }}}
}
