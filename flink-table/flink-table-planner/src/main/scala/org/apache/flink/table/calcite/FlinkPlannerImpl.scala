/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.calcite

import org.apache.flink.sql.parser.ExtendedSqlNode
import org.apache.flink.table.api.{SqlParserException, TableException, ValidationException}
import org.apache.flink.table.catalog.CatalogReader

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.RelOptTable.ViewExpander
import org.apache.calcite.plan._
import org.apache.calcite.rel.RelRoot
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.advise.{SqlAdvisor, SqlAdvisorValidator}
import org.apache.calcite.sql.parser.{SqlParser, SqlParseException => CSqlParseException}
import org.apache.calcite.sql.{SqlKind, SqlNode, SqlOperatorTable}
import org.apache.calcite.sql2rel.{SqlRexConvertletTable, SqlToRelConverter}
import org.apache.calcite.tools.{FrameworkConfig, RelConversionException}

import _root_.java.lang.{Boolean => JBoolean}
import _root_.java.util
import _root_.java.util.function.{Function => JFunction}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * NOTE: this is heavily inspired by Calcite's PlannerImpl.
  * We need it in order to share the planner between the Table API relational plans
  * and the SQL relation plans that are created by the Calcite parser.
  * The main difference is that we do not create a new RelOptPlanner in the ready() method.
  */
class FlinkPlannerImpl(
    config: FrameworkConfig,
    val catalogReaderSupplier: JFunction[JBoolean, CatalogReader],
    planner: RelOptPlanner,
    val typeFactory: FlinkTypeFactory)
  extends ViewExpander {

  val operatorTable: SqlOperatorTable = config.getOperatorTable
  /** Holds the trait definitions to be registered with planner. May be null. */
  val traitDefs: ImmutableList[RelTraitDef[_ <: RelTrait]] = config.getTraitDefs
  val parserConfig: SqlParser.Config = config.getParserConfig
  val convertletTable: SqlRexConvertletTable = config.getConvertletTable
  val sqlToRelConverterConfig: SqlToRelConverter.Config = config.getSqlToRelConverterConfig

  var validator: FlinkCalciteSqlValidator = _

  private def ready() {
    if (this.traitDefs != null) {
      planner.clearRelTraitDefs()
      for (traitDef <- this.traitDefs) {
        planner.addRelTraitDef(traitDef)
      }
    }
  }

  def getCompletionHints(sql: String, cursor: Int): Array[String] = {
    val advisorValidator = new SqlAdvisorValidator(
      operatorTable,
      catalogReaderSupplier.apply(true), // ignore cases for lenient completion
      typeFactory,
      config.getParserConfig.conformance())
    val advisor = new SqlAdvisor(advisorValidator, config.getParserConfig)
    val replaced = Array[String](null)
    val hints = advisor.getCompletionHints(sql, cursor, replaced)
      .map(item => item.toIdentifier.toString)
    hints.toArray
  }

  def parse(sql: String): SqlNode = {
    try {
      ready()
      val parser: SqlParser = SqlParser.create(sql, parserConfig)
      val sqlNode: SqlNode = parser.parseStmt
      sqlNode
    } catch {
      case e: CSqlParseException =>
        throw new SqlParserException(s"SQL parse failed. ${e.getMessage}", e)
    }
  }

  def validate(sqlNode: SqlNode): SqlNode = {
    validateInternal(sqlNode, None)
  }

  def rel(validatedSqlNode: SqlNode): RelRoot = {
    try {
      assert(validatedSqlNode != null)
      val rexBuilder: RexBuilder = createRexBuilder
      val cluster: RelOptCluster = FlinkRelOptClusterFactory.create(planner, rexBuilder)
      val catalogReader: CatalogReader = catalogReaderSupplier.apply(false)
      val sqlToRelConverter: SqlToRelConverter = new SqlToRelConverter(
        this,
        validator,
        catalogReader,
        cluster,
        convertletTable,
        sqlToRelConverterConfig)
      sqlToRelConverter.convertQuery(validatedSqlNode, false, true)
      // we disable automatic flattening in order to let composite types pass without modification
      // we might enable it again once Calcite has better support for structured types
      // root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true))

      // TableEnvironment.optimize will execute the following
      // root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel))
      // convert time indicators
      // root = root.withRel(RelTimeIndicatorConverter.convert(root.rel, rexBuilder))
    } catch {
      case e: RelConversionException => throw new TableException(e.getMessage)
    }
  }

  private def validateInternal(sqlNode: SqlNode, customPath: Option[util.List[String]]): SqlNode = {
    val catalogReader = catalogReaderSupplier.apply(false)

    val readerWithPathAdjusted = customPath.map(path =>
      new CatalogReader(
        catalogReader.getRootSchema,
        List(path, path.subList(0, 1)).asJava,
        catalogReader.getTypeFactory,
        catalogReader.getConfig
      ))
      .getOrElse(catalogReader)

    try {
      sqlNode.accept(new PreValidateReWriter(catalogReader, typeFactory))
      // do extended validation.
      sqlNode match {
        case node: ExtendedSqlNode =>
          node.validate()
        case _ =>
      }
      // no need to validate row type for DDL and insert nodes.
      if (sqlNode.getKind.belongsTo(SqlKind.DDL)
        || sqlNode.getKind == SqlKind.INSERT) {
        return sqlNode
      }
      validator = new FlinkCalciteSqlValidator(
        operatorTable,
        readerWithPathAdjusted,
        typeFactory)
      validator.setIdentifierExpansion(true)
      validator.validate(sqlNode)
    }
    catch {
      case e: RuntimeException =>
        throw new ValidationException(s"SQL validation failed. ${e.getMessage}", e)
    }
  }

  override def expandView(
      rowType: RelDataType,
      queryString: String,
      schemaPath: util.List[String],
      viewPath: util.List[String])
    : RelRoot = {
    val parsed = parse(queryString)
    val validated = validateInternal(parsed, Some(schemaPath))
    rel(validated)
  }

  private def createRexBuilder: RexBuilder = {
    new RexBuilder(typeFactory)
  }
}

object FlinkPlannerImpl {
  private def rootSchema(schema: SchemaPlus): SchemaPlus = {
    if (schema.getParentSchema == null) {
      schema
    }
    else {
      rootSchema(schema.getParentSchema)
    }
  }
}
