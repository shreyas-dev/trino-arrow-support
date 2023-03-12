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
package dev.shreyas.java.trino.util;

import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

public class HiveUtils
{
    private HiveUtils()
    {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static Type getTrinoTypeFromHiveType(String type)
    {
        switch (type) {
            case "int":
            case "integer":
            case "short":
                return IntegerType.INTEGER;
            case "float":
            case "decimal":
            case "double":
                return DoubleType.DOUBLE;
            case "timestamp":
                return TimestampType.TIMESTAMP_MILLIS;
            case "string":
            case "varchar":
            default:
                return VarcharType.createUnboundedVarcharType();
        }
    }
}
