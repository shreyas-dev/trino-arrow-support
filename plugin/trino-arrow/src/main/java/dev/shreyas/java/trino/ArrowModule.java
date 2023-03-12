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
package dev.shreyas.java.trino;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import dev.shreyas.java.trino.client.ArrowClient;
import dev.shreyas.java.trino.client.HiveClient;
import dev.shreyas.java.trino.page.ArrowPageSourceProvider;
import dev.shreyas.java.trino.split.ArrowSplitManager;
import dev.shreyas.java.trino.table.ArrowConnectorMetadata;
import io.airlift.configuration.ConfigBinder;

public class ArrowModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        ConfigBinder.configBinder(binder).bindConfig(ArrowConfig.class);
        binder.bind(ArrowClient.class).in(Scopes.SINGLETON);
        binder.bind(HiveClient.class).in(Scopes.SINGLETON);
        binder.bind(ArrowConnector.class).in(Scopes.SINGLETON);
        binder.bind(ArrowConnectorMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ArrowSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ArrowPageSourceProvider.class).in(Scopes.SINGLETON);
    }
}
