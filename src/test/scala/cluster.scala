import org.scalatest.{ FunSuite, BeforeAndAfterAll }

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.ActorSystem

import apollo._

class ClusterTests extends FunSuite with BeforeAndAfterAll {

  val cfg = ConfigFactory.load()

  implicit var sys: ActorSystem = _
  var session: Session = _

  override def beforeAll {
    sys = ActorSystem("cluster-test")
    session = new Cluster(cfg).session()
  }

  test("Options") {
    Await.result(session.options(), 2 seconds)
  }

  test("Test 2") {
    val f = session.execute("USE system;")

    println(Await.result(f, 2 seconds))
  }

}
