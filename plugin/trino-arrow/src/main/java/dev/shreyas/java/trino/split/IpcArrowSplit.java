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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Map;

public class IpcArrowSplit
        implements ConnectorSplit
{
    private final List<HostAddress> hostAddress;
    private final byte[] payload;
    private final Map<String, Object> partitionValues;

    @JsonCreator
    public IpcArrowSplit(@JsonProperty("payload") byte[] payload, @JsonProperty("hostAddress")List<HostAddress> hostAddress, @JsonProperty("partitionValues") Map<String, Object> partitionValues)
    {
        this.hostAddress = hostAddress;
        this.payload = payload;
        this.partitionValues = partitionValues;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    @JsonProperty
    public byte[] getPayload()
    {
        return payload;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return hostAddress;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @JsonProperty
    public Map<String, Object> getPartitionValues()
    {
        return partitionValues;
    }
}
