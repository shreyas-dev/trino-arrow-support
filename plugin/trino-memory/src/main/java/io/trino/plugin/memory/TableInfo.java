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
package io.trino.plugin.memory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public class TableInfo
{
    private final long id;
    private final String schemaName;
    private final String tableName;
    private final List<ColumnInfo> columns;
    private final Map<HostAddress, MemoryDataFragment> dataFragments;

    private final Optional<String> comment;

    public TableInfo(long id, String schemaName, String tableName, List<ColumnInfo> columns, Map<HostAddress, MemoryDataFragment> dataFragments, Optional<String> comment)
    {
        this.id = id;
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.columns = ImmutableList.copyOf(columns);
        this.dataFragments = ImmutableMap.copyOf(dataFragments);
        this.comment = requireNonNull(comment, "comment is null");
    }

    public long getId()
    {
        return id;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    public ConnectorTableMetadata getMetadata()
    {
        return new ConnectorTableMetadata(
                new SchemaTableName(schemaName, tableName),
                columns.stream()
                        .map(ColumnInfo::getMetadata)
                        .collect(Collectors.toList()),
                emptyMap(),
                comment);
    }

    public List<ColumnInfo> getColumns()
    {
        return columns;
    }

    public ColumnInfo getColumn(ColumnHandle handle)
    {
        return columns.stream()
                .filter(column -> column.getHandle().equals(handle))
                .collect(onlyElement());
    }

    public Map<HostAddress, MemoryDataFragment> getDataFragments()
    {
        return dataFragments;
    }

    public Optional<String> getComment()
    {
        return comment;
    }
}
