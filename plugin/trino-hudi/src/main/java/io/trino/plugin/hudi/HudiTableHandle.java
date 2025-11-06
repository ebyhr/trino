/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hudi;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.model.HoodieTableType;

import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * @param constraintColumns Used only for validation when config property hudi.query-partition-filter-required is enabled
 */
public record HudiTableHandle(
        String schemaName,
        String tableName,
        String basePath,
        HoodieTableType tableType,
        List<HiveColumnHandle> partitionColumns,
        @JsonIgnore Set<HiveColumnHandle> constraintColumns, // do not serialize constraint columns as they are not needed on workers
        TupleDomain<HiveColumnHandle> partitionPredicates,
        TupleDomain<HiveColumnHandle> regularPredicates)
        implements ConnectorTableHandle
{
    public HudiTableHandle
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(basePath, "basePath is null");
        requireNonNull(tableType, "tableType is null");
        partitionColumns = ImmutableList.copyOf(partitionColumns);
        constraintColumns = ImmutableSet.copyOf(constraintColumns);
        requireNonNull(partitionPredicates, "partitionPredicates is null");
        requireNonNull(regularPredicates, "regularPredicates is null");
    }

    public SchemaTableName schemaTableName()
    {
        return SchemaTableName.schemaTableName(schemaName, tableName);
    }

    HudiTableHandle applyPredicates(
            Set<HiveColumnHandle> constraintColumns,
            TupleDomain<HiveColumnHandle> partitionTupleDomain,
            TupleDomain<HiveColumnHandle> regularTupleDomain)
    {
        return new HudiTableHandle(
                schemaName,
                tableName,
                basePath,
                tableType,
                partitionColumns,
                constraintColumns,
                partitionPredicates.intersect(partitionTupleDomain),
                regularPredicates.intersect(regularTupleDomain));
    }

    @Override
    public String toString()
    {
        return schemaTableName().toString();
    }
}
