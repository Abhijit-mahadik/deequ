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
  def shouldReject: Boolean

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
                                                 matches: Option[String] = None,
                                                 shouldReject: Boolean = true)
  extends ColumnDefinition

private[this] case class IntColumnDefinition(
                                              name: String,
                                              isNullable: Boolean = true,
                                              minValue: Option[Int] = None,
                                              maxValue: Option[Int] = None,
                                              shouldReject: Boolean = true)
  extends NumericColumnDefinition[Int] {

  override def castExpression(): Column = {
    col(name).cast(IntegerType).as(name)
  }
}

private[this] case class LongColumnDefinition(
                                              name: String,
                                              isNullable: Boolean = true,
                                              minValue: Option[Long] = None,
                                              maxValue: Option[Long] = None,
                                              shouldReject: Boolean = true)
  extends NumericColumnDefinition[Long] {

  override def castExpression(): Column = {
    col(name).cast(LongType).as(name)
  }
}

private[this] case class ShortColumnDefinition(
                                              name: String,
                                              isNullable: Boolean = true,
                                              minValue: Option[Short] = None,
                                              maxValue: Option[Short] = None,
                                              shouldReject: Boolean = true)
  extends NumericColumnDefinition[Short] {

  override def castExpression(): Column = {
    col(name).cast(ShortType).as(name)
  }
}

private[this] case class ByteColumnDefinition(
                                                name: String,
                                                isNullable: Boolean = true,
                                                minValue: Option[Byte] = None,
                                                maxValue: Option[Byte] = None,
                                                shouldReject: Boolean = true)
  extends NumericColumnDefinition[Byte] {

  override def castExpression(): Column = {
    col(name).cast(ByteType).as(name)
  }
}

private[this] case class DecimalColumnDefinition(
                                                  name: String,
                                                  precision: Int,
                                                  scale: Int,
                                                  isNullable: Boolean = true,
                                                  shouldReject: Boolean = true)
  extends ColumnDefinition {

  override def castExpression(): Column = {
    col(name).cast(DecimalType(precision, scale)).as(name)
  }
}

private[this] case class FloatColumnDefinition(
                                                name: String,
                                                isNullable: Boolean = true,
                                                minValue: Option[Float] = None,
                                                maxValue: Option[Float] = None,
                                                shouldReject: Boolean = true)
  extends NumericColumnDefinition[Float] {

  override def castExpression(): Column = {
    col(name).cast(FloatType).as(name)
  }
}

private[this] case class DoubleColumnDefinition(
                                                 name: String,
                                                 isNullable: Boolean = true,
                                                 minValue: Option[Double] = None,
                                                 maxValue: Option[Double] = None,
                                                 shouldReject: Boolean = true)
  extends NumericColumnDefinition[Double] {

  override def castExpression(): Column = {
    col(name).cast(DoubleType).as(name)
  }
}

private[this] case class BooleanColumnDefinition(
                                                  name: String,
                                                  isNullable: Boolean = true,
                                                  shouldReject: Boolean = true)
  extends ColumnDefinition {

  override def castExpression(): Column = {
    col(name).cast(BooleanType).as(name)
  }
}

private[this] case class TimestampColumnDefinition(
                                                    name: String,
                                                    mask: String,
                                                    isNullable: Boolean = true,
                                                    shouldReject: Boolean = true)
  extends MaskColumnDefinition {

  override def castExpression(): Column = {
    unix_timestamp(col(name), mask).cast(TimestampType).as(name)
  }
}

private[this] case class DateColumnDefinition(
                                                    name: String,
                                                    mask: String,
                                                    isNullable: Boolean = true,
                                                    shouldReject: Boolean = true)
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
                        matches: Option[String] = None,
                        shouldReject: Boolean = true)
  : RowLevelSchema = {

    RowLevelSchema(columnDefinitions :+ StringColumnDefinition(name, isNullable, minLength,
      maxLength, matches, shouldReject))
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
                     maxValue: Option[Int] = None,
                     shouldReject: Boolean = true)
  : RowLevelSchema = {

    RowLevelSchema(columnDefinitions :+
      IntColumnDefinition(
        name,
        isNullable,
        minValue,
        maxValue,
        shouldReject
      )
    )
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
  def withLongColumn(
                     name: String,
                     isNullable: Boolean = true,
                     minValue: Option[Long] = None,
                     maxValue: Option[Long] = None,
                     shouldReject: Boolean = true)
  : RowLevelSchema = {

    RowLevelSchema(columnDefinitions :+
      LongColumnDefinition(
        name,
        isNullable,
        minValue,
        maxValue,
        shouldReject
      )
    )
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
                     maxValue: Option[Short] = None,
                     shouldReject: Boolean = true)
  : RowLevelSchema = {

    RowLevelSchema(columnDefinitions :+
      ShortColumnDefinition(
        name,
        isNullable,
        minValue,
        maxValue,
        shouldReject
      )
    )
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
                       maxValue: Option[Byte] = None,
                       shouldReject: Boolean = true)
  : RowLevelSchema = {

    RowLevelSchema(columnDefinitions :+
      ByteColumnDefinition(
        name,
        isNullable,
        minValue,
        maxValue,
        shouldReject
      )
    )
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
                       maxValue: Option[Float] = None,
                       shouldReject: Boolean = true)
  : RowLevelSchema = {
    RowLevelSchema(columnDefinitions :+
      FloatColumnDefinition(
        name,
        isNullable,
        minValue,
        maxValue,
        shouldReject
      )
    )
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
                        maxValue: Option[Double] = None,
                        shouldReject: Boolean = true)
  : RowLevelSchema = {
    RowLevelSchema(
      columnDefinitions :+ DoubleColumnDefinition(
        name,
        isNullable,
        minValue,
        maxValue,
        shouldReject
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
                         isNullable: Boolean = true,
                         shouldReject: Boolean = true)
  : RowLevelSchema = {
    RowLevelSchema(columnDefinitions :+
      BooleanColumnDefinition(
        name,
        isNullable,
        shouldReject
      )
    )
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
                         isNullable: Boolean = true,
                         shouldReject: Boolean = true)
  : RowLevelSchema = {

    RowLevelSchema(columnDefinitions :+
      DecimalColumnDefinition(
        name,
        precision,
        scale,
        isNullable,
        shouldReject
      )
    )
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
                           isNullable: Boolean = true,
                           shouldReject: Boolean = true)
  : RowLevelSchema = {

    RowLevelSchema(columnDefinitions :+
      TimestampColumnDefinition(
        name,
        mask,
        isNullable,
        shouldReject
      )
    )
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
                           isNullable: Boolean = true,
                           shouldReject: Boolean = true)
  : RowLevelSchema = {

    RowLevelSchema(columnDefinitions :+ DateColumnDefinition(name, mask, isNullable, shouldReject))
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
  private[this] val SHOULD_REJECT = "should_reject"

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

    val (message, valid) = toCNF(schema)
    val dataWithMatches = data
      .withColumn(MATCHES_COLUMN, message)
      .withColumn(SHOULD_REJECT, valid)

    dataWithMatches.persist(storageLevelForIntermediateResults)

    val validRows = extractAndCastValidRows(dataWithMatches, schema)
    val numValidRows = validRows.count()

    dataWithMatches.show(false)
    val invalidRows = dataWithMatches
      .where(col(SHOULD_REJECT) === lit(true))

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
        column => column != MATCHES_COLUMN && column != SHOULD_REJECT
      }
      .map { name => castExpressions.getOrElse(name, col(name)) }

    dataWithMatches.select(projection: _*).where(col(SHOULD_REJECT) === lit(false))
  }

  private[this] def toCnfFromMaskedDefinition(
                     colDef: MaskColumnDefinition,
                     colIsNull: Column): (Column, Column) = {
    val hasCorrectType = colDef.castExpression().isNotNull

    (
      when(
        colIsNull.or(
          hasCorrectType
        ),
        lit("")
      )
        .otherwise(s"${colDef.name}:D-TYPE"),
      hasCorrectType
    )
  }

  private[this] def toCnfFromColumns(columns: Column*): Column = {
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

  private[this] def toCnfFromNumericDefinition(
                     colDef: NumericColumnDefinition[_],
                     colIsNull: Column): (Column, Column) = {
    val typedColumn = colDef.castExpression()
    val hasCorrectType = colIsNull.or(typedColumn.isNotNull)
    val hasCorrectMinValue =
    colDef.minValue.map { value =>
      colIsNull.isNull.or(typedColumn.geq(value))
    }
      .getOrElse(lit(true))
    val hasCorrectMaxValue =
      colDef.maxValue.map { value =>
        colIsNull.or(typedColumn.leq(value))
      }
        .getOrElse(lit(true))

    (
      toCnfFromColumns(
        when(
          hasCorrectType,
          lit("")
        )
          .otherwise(s"${colDef.name}:D-TYPE"),
        when(
          hasCorrectMinValue,
          lit("")
        )
          .otherwise(s"${colDef.name}:MIN"),
        when(
          hasCorrectMaxValue,
          lit("")
        )
          .otherwise(s"${colDef.name}:MAX")
      ),
      hasCorrectType and hasCorrectMaxValue and hasCorrectMinValue
    )
  }

  private[this] def toCnfFromDefinition(columnDefinition: ColumnDefinition): (Column, Column) = {
    val (message, isValid): (Column, Column) = {
      val colIsNull = col(columnDefinition.name).isNull
      columnDefinition match {
        case byteDef: ByteColumnDefinition =>
          toCnfFromNumericDefinition(byteDef, colIsNull)

        case shortDef: ShortColumnDefinition =>
          toCnfFromNumericDefinition(shortDef, colIsNull)

        case intDef: IntColumnDefinition =>
          toCnfFromNumericDefinition(intDef, colIsNull)

        case longDef: LongColumnDefinition =>
          toCnfFromNumericDefinition(longDef, colIsNull)

        case decDef: DecimalColumnDefinition =>
          val decType = DataTypes.createDecimalType(decDef.precision, decDef.scale)
          val hasCorrectDataType = colIsNull.or(col(decDef.name).cast(decType).isNotNull)

          (
            when(
              hasCorrectDataType,
              lit("")
            )
              .otherwise(s"${columnDefinition.name}:D-TYPE"),
            hasCorrectDataType
          )

        case strDef: StringColumnDefinition =>
          val hasCorrectMinLength =
            strDef.minLength.map { value =>
              colIsNull.or(length(col(strDef.name)).geq(value))
            }
              .getOrElse(lit(true))
          val hasCorrectMaxLength =
            strDef.maxLength.map { value =>
              colIsNull.or(length(col(strDef.name)).leq(value))
            }
              .getOrElse(lit(true))
          val matches =
            strDef.matches.map { regex =>
              colIsNull.or(regexp_extract(col(strDef.name), regex, 0).notEqual(""))
            }
              .getOrElse(lit(true))

          (
            toCnfFromColumns(
              when(
                hasCorrectMinLength,
                lit("")
              )
                .otherwise(s"${columnDefinition.name}:MIN"),
              when(
                hasCorrectMaxLength,
                lit("")
              )
                .otherwise(s"${columnDefinition.name}:MAX"),
              when(
                matches,
                lit("")
              )
                .otherwise(s"${columnDefinition.name}:PATTERN")
            ),
            hasCorrectMaxLength and hasCorrectMinLength and matches
          )

        case tsDef: TimestampColumnDefinition =>
          toCnfFromMaskedDefinition(tsDef, colIsNull)

        case dateDef: DateColumnDefinition =>
          toCnfFromMaskedDefinition(dateDef, colIsNull)

        case floatDef: FloatColumnDefinition =>
          toCnfFromNumericDefinition(floatDef, colIsNull)

        case doubleDef: DoubleColumnDefinition =>
          toCnfFromNumericDefinition(doubleDef, colIsNull)

        case booleanDef: BooleanColumnDefinition =>
          val colAsBoolean = col(booleanDef.name).cast(BooleanType)
          val hasCorrectType = colIsNull.or(colAsBoolean.isNotNull)

          (
              when(
              hasCorrectType,
              lit("")
            )
              .otherwise(s"${columnDefinition.name}:D-TYPE"),
            hasCorrectType
          )

        case _ => (lit(""), lit(true))
      }
    }
    val isNullValid =
      (lit(columnDefinition.isNullable) and col(columnDefinition.name).isNull) or (
        lit(columnDefinition.isNullable) and col(columnDefinition.name).isNotNull
      ) or (
        lit(!columnDefinition.isNullable) and col(columnDefinition.name).isNotNull
      )

    (
      toCnfFromColumns(
        when(
          not(isNullValid),
          lit(s"${columnDefinition.name}:NULL")
        )
          .otherwise(lit("")),
        message
      ),
      // we want to reject when it is enabled and it is not valid
      lit(columnDefinition.shouldReject) and (not(isNullValid) or not(isValid))
    )
  }

  private[this] def toCNF(schema: RowLevelSchema): (Column, Column) = {
    val columns =
      schema.columnDefinitions.map(toCnfFromDefinition)

    (
      toCnfFromColumns(columns.map(_._1): _*),
      columns.map(_._2).reduce(_ or _) // if any column is reject true
    )
  }
}
