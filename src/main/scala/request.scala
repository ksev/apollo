package apollo

import apollo.protocol._

import akka.util.{ ByteString, ByteStringBuilder }

trait Request {
  private[apollo] def toFrame: apollo.protocol.Frame 
}

case object Options extends Request {

  def toFrame = 
    Frame( Version.V2REQUEST
         , Flags.NONE
         , 0
         , Opcode.OPTIONS
         , ByteString.empty )

}

case class Startup(cqlVersion: String) extends Request {

  def toFrame = { 
    val bsb = new ByteStringBuilder() 

    BodyWriter.putMap(bsb, Map("CQL_VERSION" -> cqlVersion))
    
    Frame( Version.V2REQUEST
         , Flags.NONE
         , 0
         , Opcode.STARTUP
         , bsb.result() )
  }

}

case class Query(query: String, consistency: Short = Consistency.ONE, flags: Byte = 0) extends Request {

  def toFrame = {
    val bsb = new ByteStringBuilder()

    BodyWriter.putLongString(bsb, query)
    BodyWriter.putShort(bsb, consistency)
    BodyWriter.putByte(bsb, flags)

    Frame( Version.V2REQUEST
         , Flags.NONE
         , 0
         , Opcode.QUERY
         , bsb.result() )

  }

}
