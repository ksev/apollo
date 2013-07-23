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

  implicit val timeout = Timeout(30 seconds)
  implicit val ec = sys.dispatcher

  private def reqConnection() = 
    (pool ? ConnectionRequest).mapTo[ActorRef]

  def options(query: String): Future[Map[String, Vector[String]]] = {
    val request = implicitly[RequestBuilder[Ops.Options]].build(Ops.Options)
    for {
      con <- reqConnection()
      frame <- (con ? request).mapTo[Frame]
    } yield implicitly[ResponseBuilder[Ops.Supported]].build(frame)
  }

  def execute(query: String): Future[Map[String, Vector[String]]] = {
    val optionsFrame = Frame(
      Version.V2REQUEST,
      Flags.NONE,
      0,
      Opcode.OPTIONS,
      akka.util.ByteString.empty)

    for {
      con <- (pool ? ConnectionRequest).mapTo[ActorRef]
      frame <- (con ? optionsFrame).mapTo[Frame]
    } yield implicitly[BodyBuilder[Map[String, Vector[String]]]].read(frame.body)._1

  }
}
