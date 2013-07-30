package apollo.protocol

import akka.util.{ ByteString, ByteStringBuilder }

import apollo._

case class Frame(
  version: Byte,
  flags: Byte,
  stream: Byte,
  opcode: Byte,
  body: ByteString) {

  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  def toByteString = {
    val builder = new ByteStringBuilder()
    builder.sizeHint(8 + body.length)

    builder.putByte(version)
    builder.putByte(flags)
    builder.putByte(stream)
    builder.putByte(opcode)
    builder.putInt(body.length)
    builder.append(body)

    builder.result()
  }

  def toResponse[T : ResponseReader]: T = {
    assert(version == Version.V2RESPONSE)
    implicitly[ResponseReader[T]].read(this)
  }

}

object Frame {

  def fromByteString(bs: ByteString) = {
    val bf = bs.asByteBuffer

    val version = bf.get()
    val flags = bf.get()
    val stream = bf.get()
    val opcode = bf.get()
    val length = bf.getInt()
    val body = bs.slice(8, length + 8)

    Frame( version
         , flags
         , stream
         , opcode
         , body )
  }

}

