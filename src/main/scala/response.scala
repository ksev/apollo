package apollo

trait Response
case class Supported(value: Map[String, Vector[String]]) extends Response

case object Ready extends Response
