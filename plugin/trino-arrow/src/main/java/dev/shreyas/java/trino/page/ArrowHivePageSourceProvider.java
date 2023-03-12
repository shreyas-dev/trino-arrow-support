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
package dev.shreyas.java.trino.page;

import com.google.inject.Inject;
import dev.shreyas.java.trino.ArrowConfig;
import dev.shreyas.java.trino.column.ArrowColumnHandle;
import dev.shreyas.java.trino.split.ArrowSplit;
import dev.shreyas.java.trino.table.ArrowTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import java.util.List;
import java.util.stream.Collectors;

public class ArrowHivePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final ArrowConfig arrowConfig;

    @Inject
    public ArrowHivePageSourceProvider(ArrowConfig arrowConfig)
    {
        this.arrowConfig = arrowConfig;
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns, DynamicFilter dynamicFilter)
    {
        System.out.println("getCurrentPredicate: " + dynamicFilter.getCurrentPredicate());
        System.out.println("simplify: " + dynamicFilter.getCurrentPredicate().simplify().toString());
        System.out.println("getColumnsCovered: " + dynamicFilter.getColumnsCovered());
        System.out.println("getDomains: " + dynamicFilter.getCurrentPredicate().getDomains());
        ArrowSplit arrowSplit = (ArrowSplit) split.getInfo();
        List<ArrowColumnHandle> columnHandles = columns.stream().map(column -> (ArrowColumnHandle) column).collect(Collectors.toList());
        return new ArrowHivePageSource((ArrowTableHandle) table, columnHandles, arrowSplit, arrowConfig);
    }
}
