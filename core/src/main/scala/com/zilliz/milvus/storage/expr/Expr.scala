package com.zilliz.milvus.storage.expr

import java.lang.{
  Boolean => JavaBoolean,
  Double => JavaDouble,
  Float => JavaFloat,
  Number => JavaNumber
}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.{Arrays, Locale}

/** The scalar subset parsed from a Milvus Plan.g4 filter string.
  *
  * Only Some(true) admits a row; comparisons with null remain unknown through
  * AND, OR and NOT. DataSource V2 predicates use [[PredicateExpr]] because
  * their literal typing and floating-point semantics follow Spark SQL.
  */
sealed trait Expr extends Serializable {
  def fields: Set[String]
  def evaluate(value: String => Any): Option[Boolean]
  final def matches(value: String => Any): Boolean =
    evaluate(value).contains(true)
}

object Expr {
  final case class Compare(field: String, operator: String, literal: Any)
      extends Expr {
    require(
      Set("==", "!=", "<", "<=", ">", ">=").contains(operator),
      s"Unsupported comparison '$operator'"
    )
    def fields: Set[String] = Set(field)
    def evaluate(value: String => Any): Option[Boolean] = {
      val actual = value(field)
      if (actual == null || literal == null) None
      else {
        // Milvus stores numeric floating literals as double, then casts to the
        // field's C++ type in GetValueFromProto<T>. Float fields therefore use
        // double-to-float rounding before comparison, including IN terms.
        (actual, literal) match {
          case (a: JavaFloat, b: JavaNumber) =>
            return Some(
              floating(
                a.floatValue().toDouble,
                b.doubleValue().toFloat.toDouble
              )
            )
          case (a: JavaDouble, b: JavaNumber) =>
            return Some(floating(a.doubleValue(), b.doubleValue()))
          case _ =>
        }
        val order = (actual, literal) match {
          case (a: JavaNumber, b: JavaNumber) =>
            BigDecimal(a.toString).compare(BigDecimal(b.toString))
          case (a: String, b: String) =>
            // Milvus compares UTF-8 std::string bytes. UTF-16 ordering differs
            // for supplementary characters relative to high BMP characters.
            Arrays.compareUnsigned(
              a.getBytes(UTF_8),
              b.getBytes(UTF_8)
            )
          case (a: Boolean, b: Boolean) => JavaBoolean.compare(a, b)
          case _ =>
            throw new IllegalArgumentException(
              s"Incompatible filter literal for $field"
            )
        }
        Some(operator match {
          case "==" => order == 0
          case "!=" => order != 0
          case "<"  => order < 0
          case "<=" => order <= 0
          case ">"  => order > 0
          case ">=" => order >= 0
        })
      }
    }
    private def floating(left: Double, right: Double): Boolean =
      operator match {
        case "==" => left == right
        case "!=" => left != right
        case "<"  => left < right
        case "<=" => left <= right
        case ">"  => left > right
        case ">=" => left >= right
      }
  }
  final case class In(field: String, literals: Vector[Any]) extends Expr {
    def fields: Set[String] = Set(field)
    def evaluate(value: String => Any): Option[Boolean] = {
      val results = literals.map(v => Compare(field, "==", v).evaluate(value))
      if (results.contains(Some(true))) Some(true)
      else if (results.contains(None) || value(field) == null) None
      else Some(false)
    }
  }
  final case class IsNull(field: String) extends Expr {
    def fields: Set[String] = Set(field)
    def evaluate(value: String => Any): Option[Boolean] = Some(
      value(field) == null
    )
  }
  final case class Not(child: Expr) extends Expr {
    def fields: Set[String] = child.fields
    def evaluate(value: String => Any): Option[Boolean] =
      child.evaluate(value).map(!_)
  }
  final case class And(left: Expr, right: Expr) extends Expr {
    def fields: Set[String] = left.fields ++ right.fields
    def evaluate(value: String => Any): Option[Boolean] =
      (left.evaluate(value), right.evaluate(value)) match {
        case (Some(false), _) | (_, Some(false)) => Some(false)
        case (Some(true), Some(true))            => Some(true)
        case _                                   => None
      }
  }
  final case class Or(left: Expr, right: Expr) extends Expr {
    def fields: Set[String] = left.fields ++ right.fields
    def evaluate(value: String => Any): Option[Boolean] =
      (left.evaluate(value), right.evaluate(value)) match {
        case (Some(true), _) | (_, Some(true)) => Some(true)
        case (Some(false), Some(false))        => Some(false)
        case _                                 => None
      }
  }
}

/** A bounded scalar subset of Milvus Plan.g4. Unsupported syntax is rejected
  * rather than forwarded as a filter over the already selected TopK.
  */
object PlanParser {
  def parse(text: String): Expr = new Parser(text).parse()

  private final case class Token(text: String, quoted: Boolean = false)
  private final class Parser(input: String) {
    require(
      input != null && input.length <= 65536,
      "Filter is null or too long"
    )
    private val tokens = tokenize(input)
    private var position = 0
    private var depth = 0
    private def current =
      if (position < tokens.size) tokens(position) else Token("<end>")
    private def accept(s: String): Boolean = {
      if (!current.quoted && current.text.equalsIgnoreCase(s)) {
        position += 1; true
      } else false
    }
    private def expect(s: String): Unit =
      require(accept(s), s"Expected '$s' at token $position")
    def parse(): Expr = {
      val result = or()
      require(
        position == tokens.size,
        s"Unsupported filter syntax at token $position: ${current.text}"
      )
      // AND/OR are parsed iteratively but produce left-associated trees. Check
      // that depth too before recursive field collection or evaluation runs.
      var pending = List(result -> 1)
      while (pending.nonEmpty) {
        val (expression, level) = pending.head
        pending = pending.tail
        require(level <= 64, "Filter expression depth exceeds 64")
        expression match {
          case Expr.Not(child) => pending = (child -> (level + 1)) :: pending
          case Expr.And(left, right) =>
            pending = (left -> (level + 1)) :: (right -> (level + 1)) :: pending
          case Expr.Or(left, right) =>
            pending = (left -> (level + 1)) :: (right -> (level + 1)) :: pending
          case _ =>
        }
      }
      result
    }
    private def or(): Expr = {
      var result = and()
      while (accept("or") || accept("||")) result = Expr.Or(result, and())
      result
    }
    private def and(): Expr = {
      var result = unary()
      while (accept("and") || accept("&&")) result = Expr.And(result, unary())
      result
    }
    private def unary(): Expr = {
      depth += 1
      require(depth <= 64, "Filter nesting exceeds 64")
      try {
        if (accept("not") || accept("!")) Expr.Not(unary())
        else if (accept("(")) { val e = or(); expect(")"); e }
        else predicate()
      } finally depth -= 1
    }
    private def predicate(): Expr = {
      val field = current
      require(
        !field.quoted && field.text.matches("[A-Za-z_][A-Za-z0-9_]*"),
        s"Expected a scalar field at token $position"
      )
      position += 1
      if (accept("is")) {
        val negate = accept("not")
        expect("null")
        val e = Expr.IsNull(field.text)
        if (negate) Expr.Not(e) else e
      } else {
        val negate = accept("not")
        if (accept("in")) {
          expect("[")
          val values = Vector.newBuilder[Any]
          if (!accept("]")) {
            values += literal()
            while (accept(",")) values += literal()
            expect("]")
          }
          val e = Expr.In(field.text, values.result())
          if (negate) Expr.Not(e) else e
        } else {
          require(!negate, "NOT must be followed by IN")
          val operator = current.text
          require(
            !current.quoted && Set("==", "!=", "<", "<=", ">", ">=").contains(
              operator
            ),
            s"Unsupported comparison '$operator'"
          )
          position += 1
          Expr.Compare(field.text, operator, literal())
        }
      }
    }
    private def literal(): Any = {
      val token = current
      position += 1
      if (token.quoted) token.text
      else
        token.text.toLowerCase(Locale.ROOT) match {
          case "true"  => true
          case "false" => false
          case number if number.matches("[+-]?[0-9]+") =>
            require(
              number.matches("[+-]?(?:0|[1-9][0-9]*)"),
              "Only decimal integer literals are supported"
            )
            number.toLong
          case number
              if number.matches(
                "[+-]?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)(?:[eE][+-]?[0-9]+)?"
              ) =>
            val value = number.toDouble
            require(
              JavaDouble.isFinite(value),
              "Floating filter literal is outside the double range"
            )
            value
          case other =>
            throw new IllegalArgumentException(s"Unsupported literal '$other'")
        }
    }
  }

  private def tokenize(input: String): Vector[Token] = {
    val result = Vector.newBuilder[Token]
    var i = 0
    var count = 0
    while (i < input.length) {
      if (input(i).isWhitespace) i += 1
      else {
        count += 1
        require(count <= 4096, "Filter has too many tokens")
        val c = input(i)
        if (c == '\'' || c == '"') {
          i += 1
          val text = new StringBuilder
          while (i < input.length && input(i) != c) {
            if (input(i) == '\\') {
              i += 1
              require(i < input.length, "Unterminated filter escape")
              text.append(input(i) match {
                case 'n'  => '\n'
                case 'r'  => '\r'
                case 't'  => '\t'
                case '\\' => '\\'
                case '\'' => '\''
                case '"'  => '"'
                case other =>
                  throw new IllegalArgumentException(
                    s"Unsupported escape $other"
                  )
              })
            } else {
              require(
                input(i) != '\r' && input(i) != '\n',
                "Unescaped newline in filter string"
              )
              text.append(input(i))
            }
            i += 1
          }
          require(i < input.length, "Unterminated filter string")
          i += 1
          result += Token(text.toString, quoted = true)
        } else if ("()[],".contains(c)) {
          result += Token(c.toString); i += 1
        } else if ("=!<>&|".contains(c)) {
          val start = i
          i += 1
          if (i < input.length && (input(i) == '=' || input(i) == c)) i += 1
          result += Token(input.substring(start, i))
        } else {
          val start = i
          while (
            i < input.length && !input(i).isWhitespace && !"()[],=!<>&|\"'"
              .contains(input(i))
          ) i += 1
          result += Token(input.substring(start, i))
        }
      }
    }
    result.result()
  }
}
