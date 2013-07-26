package apollo

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._

import com.typesafe.config.Config

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor._
import akka.pattern._
import akka.util.Timeout

import CassandraValue._

case class QueryException(code: Int, message: String) extends Throwable {

  override def getMessage = f"$code%x: $message%s"


}

class Session(pool: ActorRef, cfg: Config)(implicit sys: ActorSystem) {

  import apollo.protocol._
  import apollo.net.ConnectionRequest

  implicit val timeout = Timeout(30 seconds)
  implicit val ec = sys.dispatcher

  private def checkFailure(frame: Frame): Future[Frame] =
    if (frame.opcode != Opcode.ERROR) Future.successful(frame)
    else {
      val err = frame.toResponseType[apollo.Error]
      Future.failed(new QueryException(err.code, err.message))
    }

  private def request[T <: Response](req: Request): Future[T] =
    for { 
      con <- (pool ? ConnectionRequest).mapTo[ActorRef]
      frm <- (con ? req.toFrame).mapTo[Frame].flatMap(checkFailure)
    } yield frm.toResponseType[T]

  /** Receive the options that the Cassandra cluster can handle
    * @return Future of the query result
    */ 
  def options(): Future[Supported] = 
    request[Supported](Options)

  def use(keySpace: String): Future[SetKeyspace] =
    request[SetKeyspace](ParamQueryUnbounded("USE system")())

  /*
  def execute(query: String): Future[Frame] =
    for {
      con <- connection()
      frame <- (con ? Query(query).toFrame).mapTo[Frame]
    } yield frame
  */
}
