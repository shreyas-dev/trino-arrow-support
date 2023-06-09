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
package io.trino.plugin.tpcds;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.spi.NodeManager;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class TpcdsModule
        implements Module
{
    private final NodeManager nodeManager;

    public TpcdsModule(NodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(TpcdsConfig.class);
        binder.bind(NodeManager.class).toInstance(nodeManager);
        binder.bind(TpcdsSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(TpcdsMetadata.class).in(Scopes.SINGLETON);
        binder.bind(TpcdsSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(TpcdsRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(TpcdsNodePartitioningProvider.class).in(Scopes.SINGLETON);
        binder.bind(TpcdsConnector.class).in(Scopes.SINGLETON);
    }
}
