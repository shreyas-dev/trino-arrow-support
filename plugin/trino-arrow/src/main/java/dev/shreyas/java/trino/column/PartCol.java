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
package dev.shreyas.java.trino.column;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

public class PartCol
{
    private long partId;
    private String location;
    private Map<String, Object> partKeyValues;

    public PartCol(
            @JsonProperty("partId") long partId,
            @JsonProperty("location") String location)
    {
        this.partId = partId;
        this.location = location;
        this.partKeyValues = new HashMap<>();
    }

    public void addPartKeyValue(String partitionName, Object value)
    {
        this.partKeyValues.put(partitionName, value);
    }

    public String getLocation()
    {
        return location;
    }

    public Map<String, Object> getPartKeyValues()
    {
        return this.partKeyValues;
    }
}
