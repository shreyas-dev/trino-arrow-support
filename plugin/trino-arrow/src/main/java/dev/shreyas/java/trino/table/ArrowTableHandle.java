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
package dev.shreyas.java.trino.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class ArrowTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final long schemaId;
    private final String tableName;
    private final long tableId;
    private final long cdId;
    private final long sdId;
    private String[] projections;
    private String partitionQuery;
    private boolean partitionedTable;

    @JsonCreator
    public ArrowTableHandle(
            @JsonProperty("schemaId") long schemaId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableId") long tableId,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("sdId") long sdId,
            @JsonProperty("cdId") long cdId)
    {
        this.schemaId = schemaId;
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableId = tableId;
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.sdId = sdId;
        this.cdId = cdId;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public void setProjections(String[] projections)
    {
        this.projections = projections;
    }

    @JsonProperty
    public void setPartitionQuery(String partitionQuery)
    {
        this.partitionQuery = partitionQuery;
    }

    @JsonProperty
    public long getSdId()
    {
        return sdId;
    }

    @JsonProperty
    public long getCdId()
    {
        return cdId;
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        ArrowTableHandle other = (ArrowTableHandle) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName);
    }

    @Override
    public String toString()
    {
        return schemaName + ":" + tableName;
    }

    @JsonProperty
    public long getTableId()
    {
        return tableId;
    }

    @JsonProperty
    public long getSchemaId()
    {
        return schemaId;
    }

    public String getPartitionQuery()
    {
        return partitionQuery;
    }

    public boolean isPartitionedTable()
    {
        return partitionedTable;
    }

    public void setPartitionedTable(boolean partitionedTable)
    {
        this.partitionedTable = partitionedTable;
    }
}
