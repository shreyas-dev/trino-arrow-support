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

public class ArrowSplit
        implements ConnectorSplit
{
    final List<HostAddress> hostAddress;
    final String readLocation;

    @JsonCreator
    public ArrowSplit(@JsonProperty("hostAddress")List<HostAddress> hostAddress, @JsonProperty("readLocation")String readLocation)
    {
        this.hostAddress = hostAddress;
        this.readLocation = readLocation;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    @Override
    @JsonProperty
    public List<HostAddress> getAddresses()
    {
        return hostAddress;
    }

    @JsonProperty
    public String getReadLocation()
    {
        return readLocation;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}
