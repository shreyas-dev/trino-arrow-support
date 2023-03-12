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
package dev.shreyas.java.trino.split;

import com.google.inject.Inject;
import dev.shreyas.java.trino.client.ArrowClient;
import dev.shreyas.java.trino.client.HiveClient;
import dev.shreyas.java.trino.table.ArrowTableHandle;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import java.util.List;
import java.util.stream.Collectors;

public class ArrowSplitManager
        implements ConnectorSplitManager
{
    private final ArrowClient client;
    private final NodeManager nodeManager;
    private final HiveClient hiveClient;

    @Inject
    public ArrowSplitManager(ArrowClient client, HiveClient hiveClient, NodeManager nodeManager)
    {
        this.client = client;
        this.nodeManager = nodeManager;
        this.hiveClient = hiveClient;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        ArrowTableHandle arrowTableHandle = (ArrowTableHandle) connectorTableHandle;
        System.out.println("Partition Query: " + arrowTableHandle.getPartitionQuery());
        System.out.println("getCurrentPredicate: " + dynamicFilter.getCurrentPredicate());
        System.out.println("simplify: " + dynamicFilter.getCurrentPredicate().simplify().toString());
        System.out.println("getColumnsCovered: " + dynamicFilter.getColumnsCovered());
        System.out.println("getDomains: " + dynamicFilter.getCurrentPredicate().getDomains());
        List<HostAddress> addresses = nodeManager.getRequiredWorkerNodes().stream().map(Node::getHostAndPort).collect(Collectors.toList());
        List<IpcArrowSplit> arrowSplits = hiveClient.getPartitions(arrowTableHandle).stream().flatMap(
                (partition) -> client.writeAsArrowFileWriter(arrowTableHandle, partition, addresses).stream()
        ).toList();
//        List<IpcArrowSplit> arrowSplits = client.writeAsArrowFileWriter(arrowTableHandle, addresses);
        return new FixedSplitSource(arrowSplits);
    }
}
