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

import com.google.inject.Inject;
import dev.shreyas.java.trino.client.ArrowClient;
import dev.shreyas.java.trino.client.HiveClient;
import dev.shreyas.java.trino.column.ArrowColumnHandle;
import io.airlift.slice.Slice;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.type.Type;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ArrowConnectorMetadata
        implements ConnectorMetadata
{
    private final String schemaName = "users";
    private final String tableName = "user_data";
    private final ArrowClient arrowClient;
    private final HiveClient hiveClient;

    @Inject
    public ArrowConnectorMetadata(ArrowClient client, HiveClient hiveClient)
    {
        this.arrowClient = client;
        this.hiveClient = hiveClient;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        System.out.println("Hello");
        System.out.println(hiveClient.getHiveDatabases());
        return hiveClient.getHiveDatabases();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        if (schemaName.isPresent()) {
            return hiveClient.getTables(schemaName.get());
        }
        return new ArrayList<>();
    }

    @Override
    public ArrowTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return hiveClient.getArrowTableHandle(tableName);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        return hiveClient.getConnectorTableMetadata((ArrowTableHandle) table);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return hiveClient.getColumnHandles((ArrowTableHandle) tableHandle);
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((ArrowColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Optional applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        System.out.println("applyFilter constraint: " + constraint);
        System.out.println("applyFilter getSummary: " + constraint.getSummary());
        System.out.println("applyFilter getExpression: " + constraint.getExpression());
        Optional<Map<ColumnHandle, Domain>> mapDomains = constraint.getSummary().getDomains();
        ArrowTableHandle tableHandle = (ArrowTableHandle) handle;
        List<String> partValsQuery = new ArrayList<>();
        mapDomains.ifPresent((map) -> {
            map.forEach((columnHandle, domain) -> {
                ArrowColumnHandle arrowColumnHandle = (ArrowColumnHandle) columnHandle;
                System.out.println(arrowColumnHandle);
                if (arrowColumnHandle.isPartitionKey()) {
                    System.out.println("Querying partition key");
                    SortedRangeSet valueSet = (SortedRangeSet) domain.getValues();
                    List<Range> ranges = valueSet.getOrderedRanges();
                    StringBuilder stringBuilder = new StringBuilder();
                    for (Range range : ranges) {
                        Type type = range.getType();
                        System.out.println("range type: " + type.getTypeId().getId());
                        if (type.getTypeId().getId().equals("varchar") || type.getTypeId().getId().equals("date")) {
                            if (range.getLowValue().isPresent()) {
                                stringBuilder.append("PART_KEY_VAL");
                                stringBuilder.append(">");
                                if (range.isLowInclusive()) {
                                    stringBuilder.append("=");
                                }
                                stringBuilder.append("'");
                                stringBuilder.append(convertSliceToString((Slice) range.getLowValue().get()));
                                System.out.println("getLowValue : " + new String(((Slice) range.getLowValue().get()).getBytes(), Charset.defaultCharset()));
                                stringBuilder.append("'");
                            }
                            if (range.getHighValue().isPresent()) {
                                if (range.getLowValue().isPresent()) {
                                    stringBuilder.append(" AND ");
                                }
                                stringBuilder.append("PART_KEY_VAL");
                                stringBuilder.append("<");
                                if (range.isHighInclusive()) {
                                    stringBuilder.append("=");
                                }
                                stringBuilder.append("'");
                                System.out.println("high value : " + range.getHighValue().get());
                                System.out.println("high value : " + new String(((Slice) range.getHighValue().get()).getBytes(), Charset.defaultCharset()));
                                stringBuilder.append(convertSliceToString((Slice) range.getHighValue().get()));
                                stringBuilder.append("'");
                            }
                        }
                        else if (type.getTypeId().getId().equals("integer")) {
                            if (range.getLowValue().isPresent()) {
                                stringBuilder.append("PART_KEY_VAL");
                                stringBuilder.append(">");
                                if (range.isLowInclusive()) {
                                    stringBuilder.append("=");
                                }
                                stringBuilder.append(range.getLowValue().get());
                            }
                            if (range.getHighValue().isPresent()) {
                                if (range.getLowValue().isPresent()) {
                                    stringBuilder.append(" AND ");
                                }
                                stringBuilder.append("PART_KEY_VAL");
                                stringBuilder.append("<");
                                if (range.isHighInclusive()) {
                                    stringBuilder.append("=");
                                }
                                stringBuilder.append(range.getHighValue().get());
                            }
                        }
                        stringBuilder.append(" AND INTEGER_IDX=");
                        stringBuilder.append(arrowColumnHandle.getOrdinalPosition());
                        stringBuilder.append(" ");
                        partValsQuery.add(stringBuilder.toString());
                    }
                }
            });
            StringBuilder stringBuilder = new StringBuilder();
            for (String s : partValsQuery) {
                if (stringBuilder.length() != 0) {
                    stringBuilder.append(" INTERSECT ");
                }
                stringBuilder.append("SELECT PART_ID FROM PARTITION_KEY_VALS WHERE ");
                stringBuilder.append(s);
            }
            tableHandle.setPartitionQuery(stringBuilder.toString());
        });
        System.out.println("applyFilter getExpression: " + constraint.getSummary().simplify());
        System.out.println("applyFilter getExpression: " + constraint.getSummary().getDomains());
        System.out.println("applyFilter: " + constraint.getPredicateColumns());
        return Optional.of(new ConstraintApplicationResult<>(tableHandle, constraint.getSummary(), false));
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(ConnectorSession session, ConnectorTableHandle handle, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        ArrowTableHandle tableHandle = (ArrowTableHandle) handle;
        String[] colProjections = new String[assignments.size()];
        List<Assignment> assigns = new ArrayList<>();
        int i = 0;
        for (String column : assignments.keySet()) {
            colProjections[i++] = column;
            ArrowColumnHandle columnHandle = (ArrowColumnHandle) assignments.get(column);
            assigns.add(new Assignment(columnHandle.getColumnName(), columnHandle, columnHandle.getColumnType()));
        }
        tableHandle.setProjections(colProjections);
        return Optional.of(new ProjectionApplicationResult<>(tableHandle, projections, assigns, true));
    }

    public String convertSliceToString(Slice slice)
    {
        return new String(slice.getBytes(), Charset.defaultCharset());
    }
}
