/**
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 * http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package com.amazon.deequ.schema

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel


sealed trait ColumnDefinition {
  def name: String

  def isNullable: Boolean

  def castExpression(): Column = {
    col(name)
  }
}

sealed trait NumericColumnDefinition[T] extends ColumnDefinition {
  val minValue: Option[T]
  val maxValue: Option[T]
}

sealed trait MaskColumnDefinition extends ColumnDefinition {
  val mask: String
}

private[this] case class StringColumnDefinition(
                                                 name: String,
                                                 isNullable: Boolean = true,
                                                 minLength: Option[Int] = None,
                                                 maxLength: Option[Int] = None,
                                                 matches: Option[String] = None)
  extends ColumnDefinition

private[this] case class IntColumnDefinition(
                                              name: String,
                                              isNullable: Boolean = true,
                                              minValue: Option[Int] = None,
                                              maxValue: Option[Int] = None)
  extends NumericColumnDefinition[Int] {

  override def castExpression(): Column = {
    col(name).cast(IntegerType).as(name)
  }
}

private[this] case class ShortColumnDefinition(
                                              name: String,
                                              isNullable: Boolean = true,
                                              minValue: Option[Short] = None,
                                              maxValue: Option[Short] = None)
  extends NumericColumnDefinition[Short] {

  override def castExpression(): Column = {
    col(name).cast(ShortType).as(name)
  }
}

private[this] case class ByteColumnDefinition(
                                                name: String,
                                                isNullable: Boolean = true,
                                                minValue: Option[Byte] = None,
                                                maxValue: Option[Byte] = None)
  extends NumericColumnDefinition[Byte] {

  override def castExpression(): Column = {
    col(name).cast(ByteType).as(name)
  }
}

private[this] case class DecimalColumnDefinition(
                                                  name: String,
                                                  precision: Int,
                                                  scale: Int,
                                                  isNullable: Boolean = true)
  extends ColumnDefinition {

  override def castExpression(): Column = {
    col(name).cast(DecimalType(precision, scale)).as(name)
  }
}

private[this] case class FloatColumnDefinition(
                                                name: String,
                                                isNullable: Boolean = true,
                                                minValue: Option[Float] = None,
                                                maxValue: Option[Float] = None)
  extends NumericColumnDefinition[Float] {

  override def castExpression(): Column = {
    col(name).cast(FloatType).as(name)
  }
}

private[this] case class DoubleColumnDefinition(
                                                 name: String,
                                                 isNullable: Boolean = true,
                                                 minValue: Option[Double] = None,
                                                 maxValue: Option[Double] = None)
  extends NumericColumnDefinition[Double] {

  override def castExpression(): Column = {
    col(name).cast(DoubleType).as(name)
  }
}

private[this] case class BooleanColumnDefinition(
                                                  name: String,
                                                  isNullable: Boolean = true)
  extends ColumnDefinition {

  override def castExpression(): Column = {
    col(name).cast(BooleanType).as(name)
  }
}

private[this] case class TimestampColumnDefinition(
                                                    name: String,
                                                    mask: String,
                                                    isNullable: Boolean = true)
  extends MaskColumnDefinition {

  override def castExpression(): Column = {
    unix_timestamp(col(name), mask).cast(TimestampType).as(name)
  }
}

private[this] case class DateColumnDefinition(
                                                    name: String,
                                                    mask: String,
                                                    isNullable: Boolean = true)
  extends MaskColumnDefinition {

  override def castExpression(): Column = {
    to_date(col(name), mask).cast(DateType).as(name)
  }
}


/** A simple schema definition for relational data in Andes */
case class RowLevelSchema(columnDefinitions: Seq[ColumnDefinition] = Seq.empty) {

  /**
   * Declare a textual column
   *
   * @param name       column name
   * @param isNullable are NULL values permitted?
   * @param minLength  minimum length of values
   * @param maxLength  maximum length of values
   * @param matches    regular expression which the column value must match
   * @return
   */
  def withStringColumn(
                        name: String,
                        isNullable: Boolean = true,
                        minLength: Option[Int] = None,
                        maxLength: Option[Int] = None,
                        matches: Option[String] = None)
  : RowLevelSchema = {

    RowLevelSchema(columnDefinitions :+ StringColumnDefinition(name, isNullable, minLength,
      maxLength, matches))
  }

  /**
   * Declare an integer column
   *
   * @param name       column name
   * @param isNullable are NULL values permitted?
   * @param minValue   minimum value
   * @param maxValue   maximum value
   * @return
   */
  def withIntColumn(
                     name: String,
                     isNullable: Boolean = true,
                     minValue: Option[Int] = None,
                     maxValue: Option[Int] = None)
  : RowLevelSchema = {

    RowLevelSchema(columnDefinitions :+ IntColumnDefinition(name, isNullable, minValue, maxValue))
  }

  /**
   * Declare an short column
   *
   * @param name       column name
   * @param isNullable are NULL values permitted?
   * @param minValue   minimum value
   * @param maxValue   maximum value
   * @return
   */
  def withShortColumn(
                     name: String,
                     isNullable: Boolean = true,
                     minValue: Option[Short] = None,
                     maxValue: Option[Short] = None)
  : RowLevelSchema = {

    RowLevelSchema(columnDefinitions :+ ShortColumnDefinition(name, isNullable, minValue, maxValue))
  }

  /**
   * Declare an short column
   *
   * @param name       column name
   * @param isNullable are NULL values permitted?
   * @param minValue   minimum value
   * @param maxValue   maximum value
   * @return
   */
  def withByteColumn(
                       name: String,
                       isNullable: Boolean = true,
                       minValue: Option[Byte] = None,
                       maxValue: Option[Byte] = None)
  : RowLevelSchema = {

    RowLevelSchema(columnDefinitions :+ ByteColumnDefinition(name, isNullable, minValue, maxValue))
  }

  /**
   * Declare an Float column
   *
   * @param name       column name
   * @param isNullable are NULL values permitted?
   * @return
   */
  def withFloatColumn(
                       name: String,
                       isNullable: Boolean = true,
                       minValue: Option[Float] = None,
                       maxValue: Option[Float] = None)
  : RowLevelSchema = {
    RowLevelSchema(columnDefinitions :+ FloatColumnDefinition(name, isNullable, minValue, maxValue))
  }

  /**
   * Declare an Double column
   *
   * @param name       column name
   * @param isNullable are NULL values permitted?
   * @return
   */
  def withDoubleColumn(
                        name: String,
                        isNullable: Boolean = true,
                        minValue: Option[Double] = None,
                        maxValue: Option[Double] = None)
  : RowLevelSchema = {
    RowLevelSchema(
      columnDefinitions :+ DoubleColumnDefinition(
        name,
        isNullable,
        minValue,
        maxValue
      )
    )
  }

  /**
   * Declare an Boolean column
   *
   * @param name       column name
   * @param isNullable are NULL values permitted?
   * @return
   */
  def withBooleanColumn(
                         name: String,
                         isNullable: Boolean = true)
  : RowLevelSchema = {
    RowLevelSchema(columnDefinitions :+ BooleanColumnDefinition(name, isNullable))
  }



  /**
   * Declare a decimal column
   *
   * @param name       column name
   * @param precision  precision of values
   * @param scale      scale of values
   * @param isNullable are NULL values permitted?
   * @return
   */
  def withDecimalColumn(
                         name: String,
                         precision: Int,
                         scale: Int,
                         isNullable: Boolean = true)
  : RowLevelSchema = {

    RowLevelSchema(columnDefinitions :+ DecimalColumnDefinition(name, precision, scale, isNullable))
  }

  /**
   * Declare a timestamp column
   *
   * @param name       column name
   * @param mask       pattern for the timestamp
   * @param isNullable are NULL values permitted?
   * @return
   */
  def withTimestampColumn(
                           name: String,
                           mask: String,
                           isNullable: Boolean = true)
  : RowLevelSchema = {

    RowLevelSchema(columnDefinitions :+ TimestampColumnDefinition(name, mask, isNullable))
  }

  /**
   * Declare a date column
   *
   * @param name       column name
   * @param mask       pattern for the date
   * @param isNullable are NULL values permitted?
   * @return
   */
  def withDateColumn(
                           name: String,
                           mask: String,
                           isNullable: Boolean = true)
  : RowLevelSchema = {

    RowLevelSchema(columnDefinitions :+ DateColumnDefinition(name, mask, isNullable))
  }
}

/**
 * Result of enforcing a schema on textual data
 *
 * @param validRows      data frame holding the (casted) rows which conformed to the schema
 * @param numValidRows   number of rows which conformed to the schema
 * @param invalidRows    data frame holding the rows which did not conform to the schema
 * @param numInvalidRows number of rows which did not conform to the schema
 */
case class RowLevelSchemaValidationResult(
                                           validRows: DataFrame,
                                           numValidRows: Long,
                                           invalidRows: DataFrame,
                                           numInvalidRows: Long
                                         )

/** Enforce a schema on textual data */
object RowLevelSchemaValidator {

  private[this] val MATCHES_COLUMN = "validation_error"

  /**
   * Enforces a schema on textual data, filters out non-conforming columns and casts the result
   * to the requested types
   *
   * @param data a data frame holding the data to validate in string-typed columns
   * @param schema the schema to enforce
   * @param storageLevelForIntermediateResults the storage level for intermediate results
   *                                           (to control caching behavior)
   * @return results of schema enforcement
   */
  def validate(
                data: DataFrame,
                schema: RowLevelSchema,
                storageLevelForIntermediateResults: StorageLevel = StorageLevel.MEMORY_AND_DISK
              ): RowLevelSchemaValidationResult = {

    val dataWithMatches = data
      .withColumn(MATCHES_COLUMN, toCNF(schema))


    dataWithMatches.persist(storageLevelForIntermediateResults)

    val validRows = extractAndCastValidRows(dataWithMatches, schema)
    val numValidRows = validRows.count()

    dataWithMatches.show(false)
    val invalidRows = dataWithMatches
      .where(col(MATCHES_COLUMN) =!= lit(""))

    // TODO:.drop(MATCHES_COLUMN)

    val numInValidRows = invalidRows.count()

    dataWithMatches.unpersist(false)

    RowLevelSchemaValidationResult(validRows, numValidRows, invalidRows, numInValidRows)
  }

  private[this] def extractAndCastValidRows(
                                             dataWithMatches: DataFrame,
                                             schema: RowLevelSchema)
  : DataFrame = {

    val castExpressions = schema.columnDefinitions
      .map { colDef => colDef.name -> colDef.castExpression() }
      .toMap

    val projection = dataWithMatches.schema
      .map {
        _.name
      }
      .filter {
        _ != MATCHES_COLUMN
      }
      .map { name => castExpressions.getOrElse(name, col(name)) }

    dataWithMatches.select(projection: _*).where(col(MATCHES_COLUMN) === "")
  }

  private def toCnfFromMaskedDefinition(
                     colDef: MaskColumnDefinition,
                     colIsNull: Column,
                     targetType: DataType,
                     parser: (Column, String) => Column): Column = {
    when(
      colIsNull.or(
        parser(col(colDef.name), colDef.mask)
          .cast(targetType).isNotNull
      ),
      lit("")
    )
      .otherwise(s"${colDef.name}:D-TYPE")
  }

  def toCnfFromColumns(columns: Column*): Column = {
    val noEmptyColumns =
      columns
        .filter(_ != lit(""))

    if(noEmptyColumns.isEmpty) {
      lit("")
    } else {
      concat(
        noEmptyColumns
          : _*
      )
    }
  }

  private def toCnfFromNumericDefinition(
                     colDef: NumericColumnDefinition[_],
                     colIsNull: Column,
                     typedColumn: Column): Column = {
    toCnfFromColumns(
      when(
        colIsNull.or(typedColumn.isNotNull),
        lit("")
      )
        .otherwise(s"${colDef.name}:D-TYPE"),
      colDef.minValue.map { value =>
        when(
          colIsNull.isNull.or(typedColumn.geq(value)),
          lit("")
        )
          .otherwise(s"${colDef.name}:MIN")
      }
        .getOrElse(lit("")),
      colDef.maxValue.map { value =>
        when(
          colIsNull.or(typedColumn.leq(value)),
          lit("")
        )
          .otherwise(s"${colDef.name}:MAX")
      }
        .getOrElse(lit(""))
    )
  }

  private[this] def toCnfFromDefinition(columnDefinition: ColumnDefinition): Column = {
    toCnfFromColumns(
      if (!columnDefinition.isNullable) {
        when(
          col(columnDefinition.name).isNull,
          lit(s"${columnDefinition.name}:NULL")
        )
          .otherwise(lit(""))
      } else {
        lit("")
      }, {
        val colIsNull = col(columnDefinition.name).isNull
        columnDefinition match {
          case byteDef: ByteColumnDefinition =>
            toCnfFromNumericDefinition(byteDef, colIsNull, col(byteDef.name).cast(ByteType))

          case shortDef: ShortColumnDefinition =>
            toCnfFromNumericDefinition(shortDef, colIsNull, col(shortDef.name).cast(ShortType))

          case intDef: IntColumnDefinition =>
            toCnfFromNumericDefinition(intDef, colIsNull, col(intDef.name).cast(IntegerType))

          case decDef: DecimalColumnDefinition =>
            val decType = DataTypes.createDecimalType(decDef.precision, decDef.scale)

            when(
              colIsNull.or(col(decDef.name).cast(decType).isNotNull),
              lit("")
            )
              .otherwise(s"${columnDefinition.name}:D-TYPE")

          case strDef: StringColumnDefinition =>
            toCnfFromColumns(
              strDef.minLength.map { value =>
                when(
                  colIsNull.or(length(col(strDef.name)).geq(value)),
                  lit("")
                )
                  .otherwise(s"${columnDefinition.name}:MIN")
              }
                .getOrElse(lit("")),
              strDef.maxLength.map { value =>
                when(
                  colIsNull.or(length(col(strDef.name)).leq(value)),
                  lit("")
                )
                  .otherwise(s"${columnDefinition.name}:MAX")
              }
                .getOrElse(lit("")),
              strDef.matches.map { regex =>
                lit(
                  when(
                    colIsNull.or(regexp_extract(col(strDef.name), regex, 0).notEqual("")),
                    lit("")
                  )
                    .otherwise(s"${columnDefinition.name}:PATTERN")
                )
              }
                .getOrElse(lit(""))
            )

          case tsDef: TimestampColumnDefinition =>
            val parser: (Column, String) => Column = unix_timestamp
            toCnfFromMaskedDefinition(tsDef, colIsNull, TimestampType, parser)

          case dateDef: DateColumnDefinition =>
            val parser: (Column, String) => Column = to_date
            toCnfFromMaskedDefinition(dateDef, colIsNull, DateType, parser)

          case floatDef: FloatColumnDefinition =>
            toCnfFromNumericDefinition(floatDef, colIsNull, col(floatDef.name).cast(FloatType))

          case doubleDef: DoubleColumnDefinition =>
            toCnfFromNumericDefinition(doubleDef, colIsNull, col(doubleDef.name).cast(DoubleType))

          case booleanDef: BooleanColumnDefinition =>
            val colAsBoolean = col(booleanDef.name).cast(BooleanType)

            when(
              colIsNull.or(colAsBoolean.isNotNull),
              lit("")
            )
              .otherwise(s"${columnDefinition.name}:D-TYPE")

          case _ => lit("")
        }
      }
    )
  }

  private[this] def toCNF(schema: RowLevelSchema): Column = {
    val columns =
      schema.columnDefinitions.map(toCnfFromDefinition)

    toCnfFromColumns(columns: _*)
  }
}
