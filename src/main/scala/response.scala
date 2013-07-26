package apollo

trait Response
case class Supported(value: Map[String, Vector[String]]) extends Response
case class Error(code: Int, message: String) extends Response

case class SetKeyspace(keySpace: String) extends Response

case object Ready extends Response
