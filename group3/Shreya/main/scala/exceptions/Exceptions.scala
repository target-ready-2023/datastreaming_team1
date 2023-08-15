package exceptions

object Exceptions {
  case class FileReaderException(message: String) extends Exception(message)

  case class DataframeIsEmptyException(message: String) extends Exception(message)

  case class ColumnNotFoundException(message: String) extends Exception(message)

  case class EmptyFilePathException(message: String) extends Exception(message)

  case class FilePathNotFoundException(message: String) extends Exception(message)

  case class DatabaseException(message: String) extends Exception(message)

}

