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
package io.trino.parquet.reader;

import io.trino.spi.block.Block;

import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public class ColumnChunk
{
    private final Block block;
    private final int[] definitionLevels;
    private final int[] repetitionLevels;
    private OptionalLong maxBlockSize;

    public ColumnChunk(Block block, int[] definitionLevels, int[] repetitionLevels)
    {
        this(block, definitionLevels, repetitionLevels, OptionalLong.empty());
    }

    public ColumnChunk(Block block, int[] definitionLevels, int[] repetitionLevels, OptionalLong maxBlockSize)
    {
        this.block = requireNonNull(block, "block is null");
        this.definitionLevels = requireNonNull(definitionLevels, "definitionLevels is null");
        this.repetitionLevels = requireNonNull(repetitionLevels, "repetitionLevels is null");
        this.maxBlockSize = maxBlockSize;
    }

    public Block getBlock()
    {
        return block;
    }

    public int[] getDefinitionLevels()
    {
        return definitionLevels;
    }

    public int[] getRepetitionLevels()
    {
        return repetitionLevels;
    }

    public long getMaxBlockSize()
    {
        if (maxBlockSize.isEmpty()) {
            maxBlockSize = OptionalLong.of(block.getSizeInBytes());
        }
        return maxBlockSize.getAsLong();
    }
}
