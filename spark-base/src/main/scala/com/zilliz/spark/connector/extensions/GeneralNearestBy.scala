package com.zilliz.spark.connector.extensions

import scala.util.Random

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  And,
  Ascending,
  Attribute,
  AttributeMap,
  CurrentRow,
  Descending,
  EqualTo,
  Expression,
  ExpressionInfo,
  If,
  IsNotNull,
  IsNull,
  KnownNullable,
  LessThanOrEqual,
  Literal,
  NamedArgumentExpression,
  NamedExpression,
  Or,
  RowFrame,
  RowNumber,
  SortOrder,
  SpecifiedWindowFrame,
  UnboundedPreceding,
  Uuid,
  WindowExpression,
  WindowSpecDefinition
}
import org.apache.spark.sql.catalyst.expressions.FunctionTableSubqueryArgumentExpression
import org.apache.spark.sql.catalyst.expressions.RowOrdering
import org.apache.spark.sql.catalyst.plans.logical.{
  Filter,
  Join,
  JoinHint,
  LogicalPlan,
  Project,
  SubqueryAlias,
  Window
}
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{
  ByteType,
  IntegerType,
  LongType,
  ShortType,
  StringType
}
import org.apache.spark.sql.SparkSession

import com.zilliz.milvus.storage.index.RankingFunction

/** A plan with the result of Spark 4.2's `RewriteNearestByJoin`, built from
  * operators every line has, for the lines before 4.2
  * (docs/design/architecture/dataframe-api.html section 6).
  *
  * Each query row gets a `uuid()` and is joined to every base row, INNER or
  * LEFT OUTER as asked, and the ranking is computed for each pair. A window
  * over the query numbers its pairs: those with a ranking value first, by the
  * value -- ascending for a distance, descending for a similarity, NaN above
  * every number as Spark orders floats -- and the first k are kept. A LEFT
  * OUTER query whose pairs all rank NULL, or that met no base row, keeps the
  * one pair numbered first, with its base columns set to NULL. The query side
  * is read once.
  *
  * Both plans join every query to every base row; they differ in what is
  * shuffled. 4.2 keeps each query's top k in a `MaxMinByK` aggregate, whose
  * partial aggregation sends at most k base rows per query from each partition.
  * Here Spark's `InferWindowGroupLimit` puts the same limit below the window
  * only when the filter holds `position <= k` as a conjunct and k is at most
  * `spark.sql.optimizer.windowGroupLimitThreshold` (1000 by default), as an
  * INNER join's does. A LEFT OUTER filter is a disjunction, so every pair is
  * shuffled with all its query and base columns, and the window buffers all of
  * a query's pairs.
  */
private[connector] object GeneralNearestBy {

  private val random = new Random()

  def plan(request: NearestByJoinRequest): LogicalPlan = {
    val left = request.left
    val right = request.right
    val outer = request.joinType == LeftOuter
    val queryId = Alias(Uuid(Some(random.nextLong())), "__query_id")()
    val tagged = Project(left.output :+ queryId, left)
    val joined = Join(tagged, right, request.joinType, None, JoinHint.NONE)
    // A LEFT OUTER join makes the base columns nullable, and what reads them
    // says so.
    val base = AttributeMap(
      right.output.map(a => a -> (if (outer) a.withNullability(true) else a))
    )
    val ranking = Alias(
      request.rankingExpression.transform { case a: Attribute =>
        base.getOrElse(a, a)
      },
      "__ranking"
    )()
    val ranked = Project(joined.output :+ ranking, joined)
    val value = ranking.toAttribute
    val order = Seq(
      SortOrder(IsNull(value), Ascending),
      SortOrder(
        value,
        if (request.direction == RankingDirection.Distance) Ascending
        else Descending
      )
    )
    val position = Alias(
      WindowExpression(
        RowNumber(),
        WindowSpecDefinition(
          Seq(queryId.toAttribute),
          order,
          SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)
        )
      ),
      "__position"
    )()
    val windowed =
      Window(Seq(position), Seq(queryId.toAttribute), order, ranked)
    val first =
      LessThanOrEqual(position.toAttribute, Literal(request.numResults))
    val valued = And(IsNotNull(value), first)
    val kept =
      if (outer)
        Or(
          valued,
          And(IsNull(value), EqualTo(position.toAttribute, Literal(1)))
        )
      else valued
    val filtered = Filter(kept, windowed)
    // Every output column is nullable, as Spark 4.2's NearestByJoin declares.
    // The analyzer sets a column reference's nullability back to its input's
    // after this rule, so a column that is not nullable is marked so.
    def nullable(column: Attribute, value: Expression): NamedExpression =
      if (value.nullable) column
      else
        Alias(KnownNullable(value), column.name)(
          exprId = column.exprId,
          qualifier = column.qualifier
        )
    val queryColumns = left.output.map(a => nullable(a, a))
    val baseColumns = right.output.map { a =>
      val column = base(a)
      if (outer)
        Alias(
          If(IsNull(value), Literal.create(null, a.dataType), column),
          a.name
        )(
          exprId = a.exprId,
          qualifier = a.qualifier
        )
      else nullable(a, column)
    }
    Project(queryColumns ++ baseColumns, filtered)
  }
}

/** Spark's own computation of one nearest-by join on the lines before 4.2: the
  * connector's vector function, and the general plan for the query rows the
  * connector does not search.
  */
private[connector] final case class GeneralNearestBy(
    request: NearestByJoinRequest,
    function: RankingFunction
) extends SparkNearestBy {

  override def execute(
      spark: SparkSession,
      queries: RDD[InternalRow],
      queryOutput: Seq[Attribute],
      output: Seq[Attribute]
  ): RDD[InternalRow] = {
    val queryRows = Datasets.relation(spark, queryOutput, queries)
    val plan = Project(
      output,
      GeneralNearestBy.plan(request.copy(left = queryRows))
    )
    spark.sessionState.executePlan(plan).toRdd
  }
}

/** Replaces a [[NearestByJoinRequest]] once it is resolved, in the analyzer's
  * Post-Hoc Resolution batch: with [[MilvusNearestByJoin]] where the connector
  * executes the join, with the general plan everywhere else. What Spark 4.2's
  * CheckAnalysis reports about a nearest-by join is reported here, first.
  */
final class ReplaceNearestByJoinRequest(session: SparkSession)
    extends Rule[LogicalPlan]
    with Logging {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp {
    case request: NearestByJoinRequest if request.resolved =>
      NearestByArguments.checkResolved(
        request,
        SQLConf.get.crossJoinEnabled,
        RowOrdering.isOrderable(request.rankingExpression.dataType)
      )
      NearestByTakeover.replacement(
        session,
        request.left,
        request.right,
        request.joinType,
        request.approx,
        request.numResults,
        request.rankingExpression,
        request.direction,
        resolved = true,
        rankingOf(request.rankingExpression),
        approxEverywhere = true,
        (known, queryFirst) =>
          GeneralNearestBy(
            request,
            ConnectorVectorRanking(known.metric, known.name, queryFirst)
          )
      ) match {
        case Right(replaced) => replaced
        case Left(reason) =>
          logInfo(s"NEAREST BY runs as the general plan: $reason")
          GeneralNearestBy.plan(request)
      }
  }

  private def rankingOf(
      expression: Expression
  ): Option[NearestByTakeover.Ranking] = expression match {
    case function: VectorFunction =>
      Some(
        NearestByTakeover.Ranking(
          function.metric,
          function.prettyName,
          function.left,
          function.right
        )
      )
    case _ => None
  }
}

/** `nearest_by_join(TABLE(query), TABLE(base), 'ranking', num_results, mode,
  * direction[, join_type])`, the SQL form of a nearest-by join on the lines
  * before 4.2 (docs/design/architecture/dataframe-api.html section 6).
  *
  * The two `TABLE` arguments arrive as the plans the analyzer resolved, and
  * each is aliased by its side's name, so the ranking, parsed by the session's
  * parser, names query columns as `query.c` and base columns as `base.c`.
  * Arguments may be given by name.
  */
private[connector] object NearestByJoinFunction {

  val Name = "nearest_by_join"

  private val Parameters = Seq(
    "query",
    "base",
    "ranking",
    "num_results",
    "mode",
    "direction",
    "join_type"
  )

  val info: ExpressionInfo = new ExpressionInfo(
    classOf[NearestByJoinRequest].getName,
    null,
    Name,
    "_FUNC_(TABLE(query), TABLE(base), ranking, num_results, mode, direction[, join_type]) - " +
      "For each query row, the num_results base rows the ranking puts first, as Spark 4.2's " +
      "NEAREST BY join returns them.",
    "",
    "",
    "",
    "",
    "2.0.0",
    "",
    "built-in"
  )

  def builder(arguments: Seq[Expression]): LogicalPlan = {
    val named = byName(arguments)
    def required(name: String): Expression = named.getOrElse(
      name,
      throw new IllegalArgumentException(
        s"$Name needs its argument '$name'"
      )
    )
    val query = table(required("query"), "query")
    val base = table(required("base"), "base")
    val ranking = SparkSession.active.sessionState.sqlParser
      .parseExpression(text(required("ranking"), "ranking"))
    val joinType = NearestByArguments.joinType(
      named.get("join_type").map(text(_, "join_type")).getOrElse("inner")
    )
    val approx = NearestByArguments.approx(text(required("mode"), "mode"))
    val direction =
      NearestByArguments.direction(text(required("direction"), "direction"))
    val numResults = NearestByArguments.numResults(
      integer(required("num_results"), "num_results")
    )
    NearestByJoinRequest(
      SubqueryAlias("query", query),
      SubqueryAlias("base", base),
      joinType,
      approx,
      numResults,
      ranking,
      direction
    )
  }

  /** Positional arguments in parameter order, then named ones. */
  private def byName(arguments: Seq[Expression]): Map[String, Expression] = {
    val (named, positional) =
      arguments.partition(_.isInstanceOf[NamedArgumentExpression])
    require(
      positional.size <= Parameters.size,
      s"$Name takes at most ${Parameters.size} arguments, not ${positional.size}"
    )
    val byPosition = Parameters.zip(positional).toMap
    named.foldLeft(byPosition) {
      case (taken, NamedArgumentExpression(key, value)) =>
        val name = key.toLowerCase(java.util.Locale.ROOT)
        require(
          Parameters.contains(name),
          s"$Name has no argument '$key'; it takes ${Parameters.mkString(", ")}"
        )
        require(!taken.contains(name), s"$Name was given '$key' twice")
        taken + (name -> value)
      case (taken, _) => taken
    }
  }

  private def table(argument: Expression, name: String): LogicalPlan =
    argument match {
      case table: FunctionTableSubqueryArgumentExpression => table.plan
      case other =>
        throw new IllegalArgumentException(
          s"$Name takes '$name' as TABLE(...), not ${other.sql}"
        )
    }

  private def text(argument: Expression, name: String): String =
    if (argument.foldable && argument.dataType == StringType)
      Option(argument.eval()).map(_.toString).orNull
    else
      throw new IllegalArgumentException(
        s"$Name takes '$name' as a string literal, not ${argument.sql}"
      )

  private def integer(argument: Expression, name: String): Int =
    argument.dataType match {
      case ByteType | ShortType | IntegerType | LongType if argument.foldable =>
        Option(argument.eval())
          .map(value => java.lang.Math.toIntExact(value.toString.toLong))
          .getOrElse(
            throw new IllegalArgumentException(
              s"$Name takes '$name' as a number, not NULL"
            )
          )
      case _ =>
        throw new IllegalArgumentException(
          s"$Name takes '$name' as an integer literal, not ${argument.sql}"
        )
    }
}
