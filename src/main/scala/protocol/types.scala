package apollo.protocol

object Version {
  final val V2REQUEST   = 0x02.toByte
  final val V2RESPONSE  = 0x82.toByte
}

object Flags {
  final val NONE        = 0x00.toByte
  final val COMPRESSION = 0x01.toByte
  final val TRACING     = 0x02.toByte
}

object QueryFlags {
  final val NONE              = 0x00.toByte
  final val PAGE_SIZE         = 0x01.toByte
  final val VALUES            = 0x02.toByte
  final val SKIP_METADATA     = 0x04.toByte
  final val WITH_PAGING_STATE = 0x08.toByte
}

object Opcode {
  final val ERROR           = 0x00.toByte
  final val STARTUP         = 0x01.toByte
  final val READY           = 0x02.toByte
  final val AUTHENTICATE    = 0x03.toByte
  final val OPTIONS         = 0x05.toByte
  final val SUPPORTED       = 0x06.toByte
  final val QUERY           = 0x07.toByte
  final val RESULT          = 0x08.toByte
  final val EXECUTE         = 0x0A.toByte
  final val REGISTER        = 0x0B.toByte
  final val EVENT           = 0x0C.toByte
  final val BATCH           = 0x0D.toByte
  final val AUTH_CHALLENGE  = 0x0E.toByte
  final val AUTH_RESPONSE   = 0x0F.toByte
  final val AUTH_SUCCESS    = 0x10.toByte
}

object Consistency {
  final val ANY           = 0x0000.toShort
  final val ONE           = 0x0001.toShort
  final val TWO           = 0x0002.toShort
  final val THREE         = 0x0003.toShort
  final val QUORUM        = 0x0004.toShort
  final val ALL           = 0x0005.toShort
  final val LOCAL_QUORUM  = 0x0006.toShort
  final val EACH_QUORUM   = 0x0007.toShort
}

object ResultTypes {
  final val VOID          = 0x0001
  final val ROWS          = 0x0002
  final val SETKEYSPACE   = 0x0003
  final val PREPARED      = 0x0004
  final val SCHEMACHANGE  = 0x0005 
}
