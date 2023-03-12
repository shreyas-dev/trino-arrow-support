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
package io.trino.operator.aggregation;

import io.trino.operator.aggregation.state.InitialBooleanValue;
import io.trino.spi.function.AccumulatorState;

public interface LongLongState
        extends AccumulatorState
{
    long getFirst();

    void setFirst(long first);

    @InitialBooleanValue(true)
    boolean isFirstNull();

    void setFirstNull(boolean firstNull);

    long getSecond();

    void setSecond(long second);

    @InitialBooleanValue(true)
    boolean isSecondNull();

    void setSecondNull(boolean secondNull);
}
