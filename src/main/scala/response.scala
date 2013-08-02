package apollo

import apollo.protocol._

trait Response
case class Supported(value: Map[String, Vector[String]]) extends Response
case class Error(code: Int, message: String) extends Throwable with Response {

  override def getMessage = f"$code%x: $message%s"

}
trait Result extends Response
case class Ready() extends Result
case class Void() extends Result
case class Rows() extends Result
case class SetKeyspace(keyspace: String) extends Result
case class Prepared() extends Result
case class SchemaChange() extends Result

trait ResponseReader[T <: Response] {

  def read(frm: Frame): T

}

object ResponseReader extends DefaultResponseReaders

trait DefaultResponseReaders {

  implicit object supportedResponseReader extends ResponseReader[Supported] {

    def read(frame: Frame): Supported = {
      assert(frame.opcode == Opcode.SUPPORTED)
      Supported(BodyReader.getMultiMap(frame.body)._1)
    }

  }

  implicit object errorResponseReader extends ResponseReader[Error] {

    def read(frame: Frame): Error = {
      assert(frame.opcode == Opcode.ERROR)

      val (err, bs) = BodyReader.getInt(frame.body)
      val (str, _) = BodyReader.getString(bs)

      Error(err, str)
    }

  }

  implicit object readyResponseReader extends ResponseReader[Ready] {
  
    def read(frame: Frame): Ready = {
      assert(frame.opcode == Opcode.READY)
      Ready()
    }
  
  }

  implicit object voidResponseReader extends ResponseReader[Void] {
    
    def read(frame: Frame): Void = {
      assert(frame.opcode == Opcode.RESULT)
      val (rt, _) = BodyReader.getInt(frame.body)
      assert(rt == ResultTypes.VOID)
      Void()
    }

  }

  implicit object rowsResponseReader extends ResponseReader[Rows] {

    def read(frame: Frame): Rows = {
      assert(frame.opcode == Opcode.RESULT)
      val (rt, bs) = BodyReader.getInt(frame.body)
      assert(rt == ResultTypes.ROWS)
      Rows()
    }

  }

  implicit object setKeyspaceResponseReader extends ResponseReader[SetKeyspace] {

    def read(frame: Frame): SetKeyspace = {
      assert(frame.opcode == Opcode.RESULT)
      val (rt, bs) = BodyReader.getInt(frame.body)
      assert(rt == ResultTypes.SETKEYSPACE)
      SetKeyspace(BodyReader.getString(bs)._1)
    }

  }

  implicit object schemaChangeResponseReader extends ResponseReader[SchemaChange] {

    def read(frame: Frame): SchemaChange = {
      assert(frame.opcode == Opcode.RESULT)
      val (rt, bs) = BodyReader.getInt(frame.body)
      assert(rt == ResultTypes.SCHEMACHANGE)
      SchemaChange()
    }

  }

}

