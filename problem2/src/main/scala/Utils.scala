import java.util.regex.Pattern
import scala.util.matching.Regex

object Utils {
  val TuplePattern: Regex = """(?m)^\((.*),(.*)\)$""".r
  val PairPattern: Regex = """^(.*):(.*)$""".r

  //JAVA style regex (faster)
  val WORD_PATTERN: Pattern = Pattern.compile("^[a-z_\\-]{6,24}$")
  val NUMBER_PATTERN: Pattern = Pattern.compile("^-?[0-9]+([.,][0-9]+)?$")
}
