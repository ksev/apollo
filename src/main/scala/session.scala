package apollo

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._

import com.typesafe.config.Config

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor._
import akka.pattern._
import akka.util.Timeout

class Session(pool: ActorRef, cfg: Config)(implicit sys: ActorSystem) {

  import apollo.protocol._
  import apollo.net.ConnectionRequest

  implicit val timeout = Timeout(5 seconds)
  implicit val ec = sys.dispatcher

  private def connection() = 
    (pool ? ConnectionRequest).mapTo[ActorRef]

  def options(): Future[Supported] =
    for { 
      con <- connection()
      frame <- (con ? Options.toFrame).mapTo[Frame]
    } yield frame.mapTo[Supported]

  def execute(query: String): Future[Map[String, Vector[String]]] = {
    for {
      con <- connection()
      frame <- (con ? Options.toFrame).mapTo[Frame]
    } yield BodyReader.getMultiMap(frame.body)._1

  }
}
