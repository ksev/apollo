import org.scalatest.{ FunSuite, BeforeAndAfterAll }

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.ActorSystem

import apollo._

class ClusterTests extends FunSuite with BeforeAndAfterAll {

  val cfg = ConfigFactory.load()
  implicit var sys: ActorSystem = _

  override def beforeAll {
    sys = ActorSystem("cluster-test")
  }

  test("General query") {
    val cluster = new Cluster(cfg)
    val s = cluster.session()

    println(Await.result(s.options(), 5 seconds))

  }

}
