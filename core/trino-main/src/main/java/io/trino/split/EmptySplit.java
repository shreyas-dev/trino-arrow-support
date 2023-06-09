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
package io.trino.split;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorSplit;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class EmptySplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = toIntExact(ClassLayout.parseClass(EmptySplit.class).instanceSize());

    private final CatalogHandle catalogHandle;

    @JsonCreator
    public EmptySplit(
            @JsonProperty("catalogHandle") CatalogHandle catalogHandle)
    {
        this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + catalogHandle.getRetainedSizeInBytes();
    }

    @JsonProperty
    public CatalogHandle getCatalogHandle()
    {
        return catalogHandle;
    }
}
