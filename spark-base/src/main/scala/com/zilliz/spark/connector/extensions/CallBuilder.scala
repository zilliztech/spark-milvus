package com.zilliz.spark.connector.extensions

import scala.collection.JavaConverters._

import org.antlr.v4.runtime.{
  BaseErrorListener,
  CharStreams,
  CommonTokenStream,
  RecognitionException,
  Recognizer
}
import org.apache.spark.sql.types.{
  BooleanType,
  DataType,
  DoubleType,
  LongType,
  StringType
}

import com.zilliz.spark.connector.extensions.parser.{
  MilvusCallBaseVisitor,
  MilvusCallLexer,
  MilvusCallParser
}
import com.zilliz.spark.connector.procedure.{
  Parameter,
  Procedure,
  ProcedureArgs,
  Procedures
}

/** Turns the text of a `CALL milvus.system.<name>(...)` into a `CallProcedure`
  * node: the generated parser gives the tree, this visitor resolves the
  * procedure and checks the arguments against its parameter list, so a wrong
  * name, a missing or unknown argument or a wrong type fails at parse time.
  */
object CallBuilder {

  def parse(sqlText: String): CallProcedure = {
    val lexer = new MilvusCallLexer(CharStreams.fromString(sqlText))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ErrorListener)
    val parser = new MilvusCallParser(new CommonTokenStream(lexer))
    parser.removeErrorListeners()
    parser.addErrorListener(ErrorListener)
    new Visitor(sqlText).visitStatement(parser.statement())
  }

  private object ErrorListener extends BaseErrorListener {
    override def syntaxError(
        recognizer: Recognizer[_, _],
        offendingSymbol: Any,
        line: Int,
        charPositionInLine: Int,
        msg: String,
        e: RecognitionException
    ): Unit =
      throw new IllegalArgumentException(
        s"CALL syntax error at $line:$charPositionInLine: $msg"
      )
  }

  private final class Visitor(sqlText: String)
      extends MilvusCallBaseVisitor[AnyRef] {

    override def visitStatement(
        ctx: MilvusCallParser.StatementContext
    ): CallProcedure = {
      val parts = ctx.procedureName().identifier().asScala.map(identifierText)
      val procedure = resolve(parts.toSeq)
      var positional = Vector.empty[Any]
      var named = Map.empty[String, Any]
      var options = Map.empty[String, String]
      ctx.argument().asScala.foreach {
        case p: MilvusCallParser.PositionalArgumentContext =>
          if (named.nonEmpty || options.nonEmpty) {
            throw new IllegalArgumentException(
              s"procedure ${procedure.name}: positional arguments must come before named ones"
            )
          }
          positional :+= constantValue(p.constant())
        case n: MilvusCallParser.NamedArgumentContext =>
          val key = identifierText(n.identifier())
          val value = constantValue(n.constant())
          if (n.identifier().BACKQUOTED_IDENTIFIER() != null) {
            // A backquoted name is a connection or storage option key.
            if (options.contains(key)) {
              throw new IllegalArgumentException(
                s"procedure ${procedure.name}: option '$key' given twice"
              )
            }
            options += key -> optionText(procedure, key, value)
          } else {
            val lower = key.toLowerCase(java.util.Locale.ROOT)
            if (named.contains(lower)) {
              throw new IllegalArgumentException(
                s"procedure ${procedure.name}: argument '$key' given twice"
              )
            }
            named += lower -> value
          }
      }
      CallProcedure(procedure, bind(procedure, positional, named, options))
    }

    private def resolve(parts: Seq[String]): Procedure = {
      val namespace =
        parts.dropRight(1).map(_.toLowerCase(java.util.Locale.ROOT))
      val name = parts.last
      if (namespace != Procedures.Namespace) {
        throw new IllegalArgumentException(
          s"unknown procedure ${parts.mkString(".")}; procedures live under ${Procedures.Namespace.mkString(".")}"
        )
      }
      Procedures.byName(name).getOrElse {
        throw new IllegalArgumentException(
          s"unknown procedure ${parts
              .mkString(".")}; known: ${Procedures.all.map(_.name).mkString(", ")}"
        )
      }
    }

    private def bind(
        procedure: Procedure,
        positional: Seq[Any],
        named: Map[String, Any],
        options: Map[String, String]
    ): ProcedureArgs = {
      val params = procedure.parameters
      def signature =
        params
          .map(p =>
            s"${p.name} ${p.dataType.sql}${if (p.required) "" else " (optional)"}"
          )
          .mkString(", ")
      if (positional.size > params.size) {
        throw new IllegalArgumentException(
          s"procedure ${procedure.name}: ${positional.size} positional arguments, parameters: $signature"
        )
      }
      val byPosition =
        params.zip(positional).map { case (p, v) => p.name -> v }.toMap
      named.keys.foreach { key =>
        if (!params.exists(_.name == key)) {
          throw new IllegalArgumentException(
            s"procedure ${procedure.name}: unknown argument '$key'; parameters: $signature"
          )
        }
        if (byPosition.contains(key)) {
          throw new IllegalArgumentException(
            s"procedure ${procedure.name}: argument '$key' given both by position and by name"
          )
        }
      }
      val values = byPosition ++ named
      val checked = params.flatMap { p =>
        values.get(p.name) match {
          case Some(null) | None if p.required =>
            throw new IllegalArgumentException(
              s"procedure ${procedure.name}: missing argument '${p.name}'; parameters: $signature"
            )
          case Some(null) | None => None
          case Some(v) =>
            if (!accepts(p.dataType, v)) {
              throw new IllegalArgumentException(
                s"procedure ${procedure.name}: argument '${p.name}' must be ${p.dataType.sql}, got ${describe(v)}"
              )
            }
            Some(p.name -> v)
        }
      }
      ProcedureArgs(checked.toMap, options)
    }

    private def accepts(dataType: DataType, value: Any): Boolean =
      (dataType, value) match {
        case (StringType, _: String)   => true
        case (LongType, _: Long)       => true
        case (DoubleType, _: Double)   => true
        case (DoubleType, _: Long)     => true
        case (BooleanType, _: Boolean) => true
        case _                         => false
      }

    private def describe(value: Any): String = value match {
      case _: String  => "a string"
      case _: Long    => "an integer"
      case _: Double  => "a decimal"
      case _: Boolean => "a boolean"
      case other      => other.getClass.getSimpleName
    }

    private def optionText(
        procedure: Procedure,
        key: String,
        value: Any
    ): String = value match {
      case null =>
        throw new IllegalArgumentException(
          s"procedure ${procedure.name}: option '$key' must not be NULL"
        )
      case s: String => s
      case other     => other.toString
    }

    private def constantValue(ctx: MilvusCallParser.ConstantContext): Any =
      ctx match {
        case s: MilvusCallParser.StringConstantContext =>
          unquote(s.STRING().getText)
        case i: MilvusCallParser.IntegerConstantContext =>
          i.INTEGER().getText.toLong
        case d: MilvusCallParser.DecimalConstantContext =>
          d.DECIMAL().getText.toDouble
        case b: MilvusCallParser.BooleanConstantContext =>
          b.TRUE() != null
        case _: MilvusCallParser.NullConstantContext => null
      }

    private def identifierText(
        ctx: MilvusCallParser.IdentifierContext
    ): String =
      if (ctx.BACKQUOTED_IDENTIFIER() != null) {
        val raw = ctx.BACKQUOTED_IDENTIFIER().getText
        raw.substring(1, raw.length - 1).replace("``", "`")
      } else ctx.IDENTIFIER().getText

    /** Strips the quotes and undoes the two escapes the grammar allows: a
      * doubled quote and a backslash escape.
      */
    private def unquote(literal: String): String = {
      val quote = literal.charAt(0)
      val body = literal.substring(1, literal.length - 1)
      val out = new StringBuilder(body.length)
      var i = 0
      while (i < body.length) {
        val c = body.charAt(i)
        if (c == '\\' && i + 1 < body.length) {
          out.append(body.charAt(i + 1)); i += 2
        } else if (
          c == quote && i + 1 < body.length && body.charAt(i + 1) == quote
        ) {
          out.append(quote); i += 2
        } else {
          out.append(c); i += 1
        }
      }
      out.toString
    }
  }
}
