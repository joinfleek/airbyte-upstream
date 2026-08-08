/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.bigquery.typing_deduping.direct_load_tables

import io.airbyte.cdk.ConfigErrorException
import io.airbyte.cdk.load.command.Append
import io.airbyte.cdk.load.command.Dedupe
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.command.NamespaceMapper
import io.airbyte.cdk.load.config.NamespaceDefinitionType
import io.airbyte.cdk.load.data.FieldType
import io.airbyte.cdk.load.data.IntegerType
import io.airbyte.cdk.load.data.ObjectType
import io.airbyte.cdk.load.data.ObjectTypeWithoutSchema
import io.airbyte.cdk.load.data.StringType
import io.airbyte.cdk.load.data.TimestampTypeWithTimezone
import io.airbyte.cdk.load.orchestration.db.ColumnNameMapping
import io.airbyte.cdk.load.orchestration.db.TableName
import io.airbyte.integrations.destination.bigquery.spec.CdcDeletionMode
import io.airbyte.integrations.destination.bigquery.write.typing_deduping.direct_load_tables.BigqueryDirectLoadSqlGenerator
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class BigqueryDirectLoadSqlGeneratorTest {
    @Test
    fun testSoftDeletePreservesBusinessColumns() {
        val sql =
            BigqueryDirectLoadSqlGenerator("project", CdcDeletionMode.SOFT_DELETE)
                .upsertTable(
                    cdcStream(),
                    cdcColumnMapping(),
                    TableName("dataset", "source"),
                    TableName("dataset", "target"),
                )
                .transactions
                .single()
                .single()

        val softDeleteClause =
            sql.substringAfter(
                "WHEN MATCHED AND new_record._ab_cdc_deleted_at IS NOT NULL"
            ).substringBefore("WHEN MATCHED AND new_record._ab_cdc_deleted_at IS NULL")

        assertContains(softDeleteClause, "`id` = new_record.`id`,")
        assertContains(
            softDeleteClause,
            "`_ab_cdc_deleted_at` = new_record.`_ab_cdc_deleted_at`,"
        )
        assertContains(softDeleteClause, "`_ab_cdc_lsn` = new_record.`_ab_cdc_lsn`,")
        assertContains(
            softDeleteClause,
            "`payload` = IF(new_record._airbyte_has_preceding_non_delete, new_record.`payload`, target_table.`payload`),",
        )
        assertContains(
            sql,
            "WHEN MATCHED AND new_record._ab_cdc_deleted_at IS NULL AND",
        )
        assertContains(sql, "non_deleted_numbered_rows AS")
        assertContains(
            sql,
            "IF(latest.`_ab_cdc_deleted_at` IS NOT NULL AND non_deleted._airbyte_raw_id IS NOT NULL, non_deleted.`payload`, latest.`payload`) AS `payload`,",
        )
        assertContains(
            sql,
            "(non_deleted._airbyte_raw_id IS NOT NULL) AS _airbyte_has_preceding_non_delete",
        )
    }

    @Test
    fun testHardDeleteBehaviorIsUnchanged() {
        val sql =
            BigqueryDirectLoadSqlGenerator("project", CdcDeletionMode.HARD_DELETE)
                .upsertTable(
                    cdcStream(),
                    cdcColumnMapping(),
                    TableName("dataset", "source"),
                    TableName("dataset", "target"),
                )
                .transactions
                .single()
                .single()

        assertContains(
            sql,
            "WHEN MATCHED AND new_record._ab_cdc_deleted_at IS NOT NULL",
        )
        assertContains(sql, "THEN DELETE")
        assertContains(
            sql,
            "WHEN NOT MATCHED AND new_record._ab_cdc_deleted_at IS NULL THEN INSERT",
        )
        assertFalse(sql.contains("_airbyte_has_preceding_non_delete"))
    }

    @Test
    fun testClusteringColumnsAppend() {
        val clusteringColumns =
            BigqueryDirectLoadSqlGenerator.clusteringColumns(
                DestinationStream(
                    "unused",
                    "unused",
                    Append,
                    ObjectType(
                        linkedMapOf(
                            "foo" to FieldType(IntegerType, nullable = true),
                            "bar" to FieldType(IntegerType, nullable = true),
                        )
                    ),
                    generationId = 42,
                    minimumGenerationId = 0,
                    syncId = 12,
                    namespaceMapper = NamespaceMapper(NamespaceDefinitionType.SOURCE),
                ),
                ColumnNameMapping(
                    mapOf(
                        "foo" to "mapped_foo",
                        "bar" to "mapped_bar",
                    )
                )
            )
        assertEquals(listOf("_airbyte_extracted_at"), clusteringColumns)
    }

    @Test
    fun testClusteringColumnsDedup() {
        val clusteringColumns =
            BigqueryDirectLoadSqlGenerator.clusteringColumns(
                DestinationStream(
                    "unused",
                    "unused",
                    Dedupe(
                        primaryKey = listOf(listOf("foo")),
                        cursor = listOf("bar"),
                    ),
                    ObjectType(
                        linkedMapOf(
                            "foo" to FieldType(IntegerType, nullable = true),
                            "bar" to FieldType(IntegerType, nullable = true),
                        )
                    ),
                    generationId = 42,
                    minimumGenerationId = 0,
                    syncId = 12,
                    namespaceMapper = NamespaceMapper(NamespaceDefinitionType.SOURCE),
                ),
                ColumnNameMapping(
                    mapOf(
                        "foo" to "mapped_foo",
                        "bar" to "mapped_bar",
                    )
                )
            )
        assertEquals(listOf("mapped_foo", "_airbyte_extracted_at"), clusteringColumns)
    }

    @Test
    fun testClusteringColumnsFailOnJsonType() {
        val e =
            assertThrows<ConfigErrorException> {
                BigqueryDirectLoadSqlGenerator.clusteringColumns(
                    DestinationStream(
                        "ns",
                        "n",
                        Dedupe(
                            primaryKey = listOf(listOf("foo")),
                            cursor = listOf("bar"),
                        ),
                        ObjectType(
                            linkedMapOf(
                                "foo" to FieldType(ObjectTypeWithoutSchema, nullable = true),
                                "bar" to FieldType(ObjectTypeWithoutSchema, nullable = true),
                            )
                        ),
                        generationId = 42,
                        minimumGenerationId = 0,
                        syncId = 12,
                        namespaceMapper = NamespaceMapper(NamespaceDefinitionType.SOURCE),
                    ),
                    ColumnNameMapping(
                        mapOf(
                            "foo" to "mapped_foo",
                            "bar" to "mapped_bar",
                        )
                    )
                )
            }
        // note: we used unmapped column names in the exception message
        assertEquals(
            "Stream ns.n: Primary key contains non-clusterable JSON-typed column [foo]",
            e.message
        )
    }

    private fun cdcStream() =
        DestinationStream(
            "public",
            "cdc_test",
            Dedupe(primaryKey = listOf(listOf("id")), cursor = listOf("_ab_cdc_lsn")),
            ObjectType(
                linkedMapOf(
                    "id" to FieldType(IntegerType, nullable = false),
                    "payload" to FieldType(StringType, nullable = true),
                    "_ab_cdc_updated_at" to
                        FieldType(TimestampTypeWithTimezone, nullable = true),
                    "_ab_cdc_deleted_at" to
                        FieldType(TimestampTypeWithTimezone, nullable = true),
                    "_ab_cdc_lsn" to FieldType(IntegerType, nullable = true),
                )
            ),
            generationId = 42,
            minimumGenerationId = 0,
            syncId = 12,
            namespaceMapper = NamespaceMapper(NamespaceDefinitionType.SOURCE),
        )

    private fun cdcColumnMapping() =
        ColumnNameMapping(
            mapOf(
                "id" to "id",
                "payload" to "payload",
                "_ab_cdc_updated_at" to "_ab_cdc_updated_at",
                "_ab_cdc_deleted_at" to "_ab_cdc_deleted_at",
                "_ab_cdc_lsn" to "_ab_cdc_lsn",
            )
        )
}
