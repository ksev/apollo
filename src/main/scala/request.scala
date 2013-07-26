package apollo

import apollo.protocol._

import akka.util.{ ByteString, ByteStringBuilder }

case class CassandraValue[T](value: T, marshal: CassandraMarshal[T])
object CassandraValue {

  implicit def valueToCassandraValue[T: CassandraMarshal](value: T) =
    CassandraValue(value, implicitly[CassandraMarshal[T]])

}

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

case class RawQuery(
  query: String, 
  consistency: Short = Consistency.ONE, 
  flags: Byte = 0,
  pageSize: Option[Int] = None) 
  extends Request {

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

case class ParamQueryUnbounded(
  query: String, 
  consistency: Short = Consistency.ONE, 
  flags: Byte = 0,
  pageSize: Option[Int] = None) {

  def apply(params: CassandraValue[_]*): ParamQueryBounded =
    ParamQueryBounded( query
                     , params
                     , consistency
                     , flags
                     , pageSize )

}

case class ParamQueryBounded(
  query: String,
  params: Seq[CassandraValue[_]],
  consistency: Short = Consistency.ONE,
  flags: Byte = 0,
  pageSize: Option[Int] = None)
  extends Request {

  def toFrame = {
    val bsb = new ByteStringBuilder()

    BodyWriter.putLongString(bsb, query)
    BodyWriter.putShort(bsb, consistency)
    BodyWriter.putByte(bsb, (flags | QueryFlags.VALUES).toByte)
    BodyWriter.putShort(bsb, params.length.toShort)
    
    BodyWriter.putBytes(bsb, Some(ByteString("system", ("UTF-8"))))

    Frame( Version.V2REQUEST
         , Flags.NONE
         , 0
         , Opcode.QUERY
         , bsb.result() )
  }
   
}
