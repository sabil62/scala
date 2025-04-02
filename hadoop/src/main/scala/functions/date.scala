package functions

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.{Try, Success, Failure}

object date {
  def formatDate(
      dateVariable: Any,
      formatter: DateTimeFormatter
  ): LocalDateTime = {
    dateVariable match {
      case d: LocalDateTime => d
      case s: String =>
        Try(LocalDateTime.parse(s, formatter)) match {
          case Success(parsedDate) => parsedDate
          case Failure(ex) =>
            throw new IllegalArgumentException(
              s"Unable to parse date string: $s",
              ex
            )
        }
      case null =>
        throw new IllegalArgumentException("The date provided is null")
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported date type: ${date.getClass.getSimpleName}"
        )
    }
  }
}
