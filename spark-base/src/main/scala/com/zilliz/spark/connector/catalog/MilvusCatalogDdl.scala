package com.zilliz.spark.connector.catalog

import java.util.Locale
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.types.{
  ArrayType,
  BinaryType,
  BooleanType,
  ByteType,
  DataType => SparkDataType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  MapType,
  ShortType,
  StringType
}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.client.api.MilvusClient
import io.milvus.grpc.schema.{DataType => MilvusDataType}

/** The Spark-line-independent input to catalog CREATE TABLE. */
private[catalog] final case class MilvusCatalogCreate(
    identifier: Identifier,
    columns: Seq[MilvusCatalogColumn],
    hasPartitionTransforms: Boolean,
    hasConstraints: Boolean,
    properties: Map[String, String]
)

private[catalog] final case class MilvusCatalogColumn(
    name: String,
    dataType: SparkDataType,
    nullable: Boolean,
    comment: Option[String],
    hasDefault: Boolean = false,
    generationExpression: Option[String] = None,
    hasIdentity: Boolean = false
)

/** A fully validated collection definition. No remote call is made until this
  * value has been built, including every index definition.
  */
private[catalog] final case class MilvusCatalogCollection(
    database: String,
    collection: String,
    description: String,
    fields: Seq[MilvusCatalogField],
    indexes: Seq[MilvusCatalogIndex]
)

private[catalog] final case class MilvusCatalogField(
    name: String,
    dataType: MilvusDataType,
    nullable: Boolean,
    description: String,
    primaryKey: Boolean,
    elementType: MilvusDataType = MilvusDataType.None,
    typeParameters: Map[String, String] = Map.empty
)

private[catalog] final case class MilvusCatalogIndex(
    field: String,
    name: String,
    parameters: Map[String, String]
)

/** Injectable boundary around one short-lived online client. Tests can observe
  * close directly without replacing the production MilvusClient.
  */
private[catalog] trait MilvusCatalogDdlClient extends AutoCloseable {
  def databaseExists(database: String): Boolean
  def collectionExists(database: String, collection: String): Boolean
  def createCollection(request: MilvusCatalogCollection): Unit
  def createIndex(
      request: MilvusCatalogCollection,
      index: MilvusCatalogIndex
  ): Unit
  def dropCollection(database: String, collection: String): Unit
}

private[catalog] trait MilvusCatalogDdlClientFactory {
  def open(options: CaseInsensitiveStringMap): MilvusCatalogDdlClient
}

private[catalog] object MilvusCatalogDdlClientFactory {
  val default: MilvusCatalogDdlClientFactory =
    new MilvusCatalogDdlClientFactory {
      override def open(
          options: CaseInsensitiveStringMap
      ): MilvusCatalogDdlClient =
        new DefaultMilvusCatalogDdlClient(
          MilvusClient(MilvusCatalogDiscovery.connectionParams(options))
        )
    }
}

private final class DefaultMilvusCatalogDdlClient(client: MilvusClient)
    extends MilvusCatalogDdlClient {

  override def databaseExists(database: String): Boolean =
    client.listDatabases().map(_.contains(database)).get

  override def collectionExists(
      database: String,
      collection: String
  ): Boolean =
    client
      .getCollectionInfo(database, collection)
      .map(_ => true)
      .recover { case error if MilvusCatalogBase.isMissing(error) => false }
      .get

  override def createCollection(request: MilvusCatalogCollection): Unit = {
    val fields = request.fields.map { field =>
      client.createCollectionField(
        name = field.name,
        isPrimary = field.primaryKey,
        description = field.description,
        dataType = field.dataType,
        typeParams = field.typeParameters,
        elementType = field.elementType,
        nullable = field.nullable
      )
    }
    val schema = client.createCollectionSchema(
      dbName = request.database,
      name = request.collection,
      description = request.description,
      fields = fields
    )
    client
      .createCollection(
        dbName = request.database,
        collectionName = request.collection,
        schema = schema
      )
      .get
  }

  override def createIndex(
      request: MilvusCatalogCollection,
      index: MilvusCatalogIndex
  ): Unit =
    client
      .createIndex(
        dbName = request.database,
        collectionName = request.collection,
        fieldName = index.field,
        params = index.parameters,
        indexName = index.name
      )
      .get

  override def dropCollection(
      database: String,
      collection: String
  ): Unit = client.dropCollection(database, collection).get

  override def close(): Unit = client.close()
}

private[catalog] object MilvusCatalogDdl {
  val PrimaryKeyProperty = "milvus.primary.key"
  val FieldPropertyPrefix = "milvus.field."
  val IndexPropertyPrefix = "milvus.index."

  private val DataTypeProperty = "data_type"
  private val DimensionProperty = "dim"
  private val MaxLengthProperty = "max_length"
  private val MaxCapacityProperty = "max_capacity"
  private val FieldPropertyNames = Set(
    DataTypeProperty,
    DimensionProperty,
    MaxLengthProperty,
    MaxCapacityProperty
  )
  private val SparkProperties = Set("comment", "owner", "provider")
  private val mapper = new ObjectMapper()
    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
    .enable(FAIL_ON_TRAILING_TOKENS)

  def normalize(request: MilvusCatalogCreate): MilvusCatalogCollection = {
    val (database, collection) = identifier(request.identifier)
    if (request.hasPartitionTransforms) {
      fail("Spark partition transforms are not supported")
    }
    if (request.hasConstraints) {
      fail("Spark table constraints are not supported")
    }
    if (request.columns.isEmpty)
      fail("CREATE TABLE requires at least one field")

    val duplicateFields = request.columns
      .groupBy(_.name)
      .collect { case (name, occurrences) if occurrences.size > 1 => name }
      .toSeq
      .sorted
    if (duplicateFields.nonEmpty) {
      fail(s"Duplicate field name(s): ${duplicateFields.mkString(", ")}")
    }
    request.columns.foreach(validateColumnShape)

    val parsed =
      parseProperties(request.properties, request.columns.map(_.name).toSet)
    val primaryName = parsed.primaryKey
    val fields = request.columns.map { column =>
      field(
        column,
        column.name == primaryName,
        parsed.fieldProperties(column.name)
      )
    }
    val primary = fields
      .find(_.primaryKey)
      .getOrElse(
        fail(s"Property '$PrimaryKeyProperty' must name an existing field")
      )
    if (primary.nullable) {
      fail(s"Primary-key field '${primary.name}' must be NOT NULL")
    }
    if (
      primary.dataType != MilvusDataType.Int64 &&
      primary.dataType != MilvusDataType.VarChar
    ) {
      fail(
        s"Primary-key field '${primary.name}' must map to int64 or varchar"
      )
    }

    val vectorFields = fields.filter(field => isVector(field.dataType))
    val indexes = vectorFields.map { vector =>
      parsed.indexProperties
        .get(vector.name)
        .map(parseIndex(vector.name, _))
        .getOrElse(
          fail(
            s"Vector field '${vector.name}' requires property '$IndexPropertyPrefix${vector.name}'"
          )
        )
    }
    val nonVectorIndexes = parsed.indexProperties.keySet -- vectorFields
      .map(_.name)
      .toSet
    if (nonVectorIndexes.nonEmpty) {
      fail(
        s"Index properties refer to non-vector field(s): ${nonVectorIndexes.toSeq.sorted.mkString(", ")}"
      )
    }
    val duplicateIndexNames = indexes
      .filter(_.name.nonEmpty)
      .groupBy(_.name)
      .collect { case (name, definitions) if definitions.size > 1 => name }
      .toSeq
      .sorted
    if (duplicateIndexNames.nonEmpty) {
      fail(
        s"Duplicate vector index name(s): ${duplicateIndexNames.mkString(", ")}"
      )
    }

    MilvusCatalogCollection(
      database = database,
      collection = collection,
      description = parsed.description,
      fields = fields,
      indexes = indexes
    )
  }

  private final case class ParsedProperties(
      primaryKey: String,
      description: String,
      fieldProperties: Map[String, Map[String, String]],
      indexProperties: Map[String, String]
  )

  private def parseProperties(
      properties: Map[String, String],
      fields: Set[String]
  ): ParsedProperties = {
    val safe = Option(properties).getOrElse(Map.empty)
    val fieldProperties = scala.collection.mutable.Map.empty[
      String,
      scala.collection.mutable.Map[String, String]
    ]
    val indexes = scala.collection.mutable.Map.empty[String, String]
    var primary: Option[String] = None
    var description = ""

    safe.foreach { case (rawKey, rawValue) =>
      val key =
        Option(rawKey).getOrElse(fail("Table property name must not be null"))
      val value = Option(rawValue).getOrElse(
        fail(s"Table property '$key' must not have a null value")
      )
      if (key == PrimaryKeyProperty) {
        primary = Some(nonBlank(value, s"Property '$key'"))
      } else if (key == "comment") {
        description = value
      } else if (key == "provider") {
        if (!value.equalsIgnoreCase("milvus")) {
          fail(s"Unsupported table provider '$value'; expected 'milvus'")
        }
      } else if (key == "owner") {
        ()
      } else if (key.startsWith(FieldPropertyPrefix)) {
        val (fieldName, propertyName) = splitFieldProperty(key)
        if (!fields(fieldName)) {
          fail(s"Property '$key' refers to unknown field '$fieldName'")
        }
        val values = fieldProperties.getOrElseUpdate(
          fieldName,
          scala.collection.mutable.Map.empty
        )
        values += propertyName -> value
      } else if (key.startsWith(IndexPropertyPrefix)) {
        val fieldName = key.substring(IndexPropertyPrefix.length)
        if (fieldName.isEmpty || !fields(fieldName)) {
          fail(s"Property '$key' refers to unknown field '$fieldName'")
        }
        indexes += fieldName -> value
      } else if (key.startsWith("milvus.")) {
        fail(s"Unknown Milvus table property '$key'")
      } else if (!SparkProperties(key)) {
        fail(s"Unsupported table property '$key'")
      }
    }

    val primaryName = primary.getOrElse(
      fail(s"Required table property '$PrimaryKeyProperty' is missing")
    )
    if (!fields(primaryName)) {
      fail(
        s"Property '$PrimaryKeyProperty' refers to unknown field '$primaryName'"
      )
    }
    ParsedProperties(
      primaryName,
      description,
      fields.iterator.map { name =>
        name -> fieldProperties.get(name).map(_.toMap).getOrElse(Map.empty)
      }.toMap,
      indexes.toMap
    )
  }

  private def splitFieldProperty(key: String): (String, String) = {
    val body = key.substring(FieldPropertyPrefix.length)
    val separator = body.lastIndexOf('.')
    if (separator <= 0 || separator == body.length - 1) {
      fail(s"Malformed Milvus field property '$key'")
    }
    val field = body.substring(0, separator)
    val property = body.substring(separator + 1)
    if (!FieldPropertyNames(property)) {
      fail(s"Unknown Milvus field property '$key'")
    }
    field -> property
  }

  private def validateColumnShape(column: MilvusCatalogColumn): Unit = {
    if (column.name == null || column.name.trim.isEmpty) {
      fail("Field name must not be empty")
    }
    if (column.dataType == null) fail(s"Field '${column.name}' has no type")
    if (column.hasDefault) {
      fail(s"Default values are not supported for field '${column.name}'")
    }
    if (column.generationExpression.exists(_.trim.nonEmpty)) {
      fail(s"Generated columns are not supported for field '${column.name}'")
    }
    if (column.hasIdentity) {
      fail(s"Identity columns are not supported for field '${column.name}'")
    }
  }

  private def field(
      column: MilvusCatalogColumn,
      primary: Boolean,
      properties: Map[String, String]
  ): MilvusCatalogField = {
    val declared = properties.get(DataTypeProperty).map { value =>
      nonBlank(value, s"Field '${column.name}' data_type")
        .toLowerCase(Locale.ROOT)
    }
    val mapped = (column.dataType, declared) match {
      case (BooleanType, None) => scalar(MilvusDataType.Bool)
      case (ByteType, None)    => scalar(MilvusDataType.Int8)
      case (ShortType, None)   => scalar(MilvusDataType.Int16)
      case (IntegerType, None) => scalar(MilvusDataType.Int32)
      case (LongType, None)    => scalar(MilvusDataType.Int64)
      case (FloatType, None)   => scalar(MilvusDataType.Float)
      case (DoubleType, None)  => scalar(MilvusDataType.Double)
      case (StringType, Some("varchar")) =>
        scalar(
          MilvusDataType.VarChar,
          Map(
            "max_length" -> positive(
              properties,
              MaxLengthProperty,
              column.name
            ).toString
          )
        )
      case (StringType, Some("text")) => scalar(MilvusDataType.Text)
      case (StringType, Some("json")) => scalar(MilvusDataType.JSON)
      case (array: ArrayType, Some("array")) =>
        arrayField(array, properties, column.name)
      case (ArrayType(FloatType, _), Some("float_vector")) =>
        denseVector(MilvusDataType.FloatVector, properties, column.name)
      case (ArrayType(FloatType, _), Some("float16_vector")) =>
        denseVector(MilvusDataType.Float16Vector, properties, column.name)
      case (ArrayType(FloatType, _), Some("bfloat16_vector")) =>
        denseVector(MilvusDataType.BFloat16Vector, properties, column.name)
      case (ArrayType(ShortType, _), Some("int8_vector")) =>
        denseVector(MilvusDataType.Int8Vector, properties, column.name)
      case (BinaryType, Some("binary_vector")) =>
        val result = denseVector(
          MilvusDataType.BinaryVector,
          properties,
          column.name
        )
        val dimension = result._3("dim").toInt
        if (dimension % 8 != 0) {
          fail(
            s"Binary vector field '${column.name}' dim must be a multiple of 8"
          )
        }
        result
      case (
            MapType(LongType, FloatType, _),
            Some("sparse_float_vector")
          ) =>
        scalar(MilvusDataType.SparseFloatVector)
      case (
            StringType | _: ArrayType | BinaryType | _: MapType,
            None
          ) =>
        fail(
          s"Field '${column.name}' with Spark type ${column.dataType.sql} requires '$FieldPropertyPrefix${column.name}.$DataTypeProperty'"
        )
      case (_, Some(dataType)) =>
        fail(
          s"Milvus data_type '$dataType' is incompatible with Spark field '${column.name}' of type ${column.dataType.sql}"
        )
      case _ =>
        fail(
          s"Unsupported Spark type for field '${column.name}': ${column.dataType.sql}"
        )
    }

    val (dataType, elementType, typeParameters) = mapped
    val allowed = dataType match {
      case MilvusDataType.VarChar => Set(DataTypeProperty, MaxLengthProperty)
      case MilvusDataType.Array =>
        Set(DataTypeProperty, MaxCapacityProperty) ++
          (if (elementType == MilvusDataType.VarChar) Set(MaxLengthProperty)
           else Set.empty)
      case value if isDenseVector(value) =>
        Set(DataTypeProperty, DimensionProperty)
      case MilvusDataType.Text | MilvusDataType.JSON |
          MilvusDataType.SparseFloatVector =>
        Set(DataTypeProperty)
      case _ => Set.empty[String]
    }
    val unexpected = properties.keySet -- allowed
    if (unexpected.nonEmpty) {
      fail(
        s"Field '${column.name}' does not accept property/properties: ${unexpected.toSeq.sorted.mkString(", ")}"
      )
    }
    MilvusCatalogField(
      name = column.name,
      dataType = dataType,
      nullable = column.nullable,
      description = column.comment.getOrElse(""),
      primaryKey = primary,
      elementType = elementType,
      typeParameters = typeParameters
    )
  }

  private def scalar(
      dataType: MilvusDataType,
      parameters: Map[String, String] = Map.empty
  ): (MilvusDataType, MilvusDataType, Map[String, String]) =
    (dataType, MilvusDataType.None, parameters)

  private def denseVector(
      dataType: MilvusDataType,
      properties: Map[String, String],
      fieldName: String
  ): (MilvusDataType, MilvusDataType, Map[String, String]) =
    scalar(
      dataType,
      Map("dim" -> positive(properties, DimensionProperty, fieldName).toString)
    )

  private def arrayField(
      array: ArrayType,
      properties: Map[String, String],
      fieldName: String
  ): (MilvusDataType, MilvusDataType, Map[String, String]) = {
    val elementType = array.elementType match {
      case BooleanType => MilvusDataType.Bool
      case ByteType =>
        fail(
          s"Array<Byte> field '$fieldName' is not supported because Milvus Int8 arrays are read as Array<Short>"
        )
      case ShortType   => MilvusDataType.Int16
      case IntegerType => MilvusDataType.Int32
      case LongType    => MilvusDataType.Int64
      case FloatType   => MilvusDataType.Float
      case DoubleType  => MilvusDataType.Double
      case StringType  => MilvusDataType.VarChar
      case other =>
        fail(
          s"Unsupported array element type for field '$fieldName': ${other.sql}"
        )
    }
    val parameters = Map(
      "max_capacity" -> positive(
        properties,
        MaxCapacityProperty,
        fieldName
      ).toString
    ) ++ (if (elementType == MilvusDataType.VarChar) {
            Map(
              "max_length" -> positive(
                properties,
                MaxLengthProperty,
                fieldName
              ).toString
            )
          } else Map.empty)
    (MilvusDataType.Array, elementType, parameters)
  }

  private def positive(
      properties: Map[String, String],
      name: String,
      fieldName: String
  ): Int = {
    val value = properties.getOrElse(
      name,
      fail(s"Field '$fieldName' requires property '$name'")
    )
    if (!value.matches("[1-9][0-9]*")) {
      fail(s"Field '$fieldName' property '$name' must be a positive integer")
    }
    try value.toInt
    catch {
      case _: NumberFormatException =>
        fail(s"Field '$fieldName' property '$name' exceeds Int range")
    }
  }

  private def parseIndex(field: String, json: String): MilvusCatalogIndex = {
    val root =
      try mapper.readTree(json)
      catch {
        case error: Exception =>
          throw new IllegalArgumentException(
            s"Invalid vector index JSON for field '$field': ${error.getMessage}",
            error
          )
      }
    if (root == null || !root.isObject) {
      fail(s"Vector index for field '$field' must be a JSON object")
    }
    val entries = root.fields().asScala.toSeq
    entries.foreach { entry =>
      val value = entry.getValue
      if (
        entry.getKey.trim.isEmpty || value == null || value.isNull ||
        (!value.isTextual && !value.isNumber && !value.isBoolean)
      ) {
        fail(
          s"Vector index '$field' entry '${entry.getKey}' must be a non-null scalar"
        )
      }
    }
    def requiredString(name: String): String = {
      val value = Option(root.get(name)).getOrElse(
        fail(s"Vector index for field '$field' requires string '$name'")
      )
      if (!value.isTextual || value.textValue().trim.isEmpty) {
        fail(
          s"Vector index for field '$field' requires non-empty string '$name'"
        )
      }
      value.textValue()
    }
    val indexType = requiredString("index_type")
    val metricType = requiredString("metric_type")
    val indexName = Option(root.get("index_name")) match {
      case None => ""
      case Some(value) if value.isTextual && value.textValue().trim.nonEmpty =>
        value.textValue()
      case _ =>
        fail(
          s"Vector index for field '$field' has an invalid 'index_name'; expected a non-empty string"
        )
    }
    val parameters = entries.collect {
      case entry if entry.getKey != "index_name" =>
        entry.getKey -> scalarText(entry.getValue)
    }.toMap ++ Map("index_type" -> indexType, "metric_type" -> metricType)
    MilvusCatalogIndex(field, indexName, parameters)
  }

  private def scalarText(value: JsonNode): String =
    if (value.isTextual) value.textValue() else value.asText()

  private def identifier(identifier: Identifier): (String, String) = {
    if (identifier == null) fail("Milvus table identifier must not be null")
    val namespace = Option(identifier.namespace()).getOrElse(Array.empty)
    val database =
      if (namespace.length == 1) Option(namespace(0)).getOrElse("") else ""
    val collection = Option(identifier.name()).getOrElse("")
    if (
      namespace.length != 1 || database.trim.isEmpty || collection.trim.isEmpty
    ) {
      fail("Milvus CREATE TABLE requires catalog.<database>.<collection>")
    }
    database -> collection
  }

  private def nonBlank(value: String, description: String): String = {
    val normalized = Option(value).map(_.trim).getOrElse("")
    if (normalized.isEmpty) fail(s"$description must not be empty")
    normalized
  }

  private def isDenseVector(dataType: MilvusDataType): Boolean =
    dataType == MilvusDataType.FloatVector ||
      dataType == MilvusDataType.Float16Vector ||
      dataType == MilvusDataType.BFloat16Vector ||
      dataType == MilvusDataType.Int8Vector ||
      dataType == MilvusDataType.BinaryVector

  private def isVector(dataType: MilvusDataType): Boolean =
    isDenseVector(dataType) || dataType == MilvusDataType.SparseFloatVector

  private def fail(message: String): Nothing =
    throw new IllegalArgumentException(message)
}
