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
package dev.shreyas.java.trino.page;

import com.google.common.collect.ImmutableList;
import dev.shreyas.java.trino.column.ArrowColumnHandle;
import dev.shreyas.java.trino.split.IpcArrowSplit;
import dev.shreyas.java.trino.table.ArrowTableHandle;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.apache.arrow.vector.util.TransferPair;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.LongTimestampWithTimeZone.fromEpochMillisAndFraction;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static org.apache.arrow.vector.complex.BaseRepeatedValueVector.OFFSET_WIDTH;
import static org.apache.arrow.vector.types.Types.MinorType.DECIMAL256;
import static org.apache.arrow.vector.types.Types.MinorType.LIST;
import static org.apache.arrow.vector.types.Types.MinorType.STRUCT;

public class ArrowPageSource
        implements ConnectorPageSource
{
    private final List<ArrowColumnHandle> columns;
    private ArrowFileReader reader;
    private long blockCount;
    private final AtomicInteger readBlocks;
    private BufferAllocator allocator;
    private List<ArrowBlock> arrowBlocks;
    private final AtomicLong readBytes = new AtomicLong();
    private final PageBuilder pageBuilder;
    private final Map<String, Object> partitionValues;

    public ArrowPageSource(ArrowTableHandle tableHandle, List<ArrowColumnHandle> columns, IpcArrowSplit ipcArrowSplit)
    {
        this.columns = columns;
        this.pageBuilder = new PageBuilder(columns.stream().map(ArrowColumnHandle::getColumnType).collect(Collectors.toList()));
        this.allocator = new RootAllocator();
        this.reader = new ArrowFileReader(new ByteArrayReadableSeekableByteChannel(ipcArrowSplit.getPayload()), allocator);
        this.readBlocks = new AtomicInteger();
        this.partitionValues = ipcArrowSplit.getPartitionValues();
        try {
            this.arrowBlocks = reader.getRecordBlocks();
            this.blockCount = this.arrowBlocks.size();
        }
        catch (IOException e) {
            this.arrowBlocks = new ArrayList<>();
            this.blockCount = 0;
            throw new RuntimeException(e);
        }
        System.out.println("ArrowBlocks Size: " + this.arrowBlocks.size());
        System.out.println("blockCount: " + this.blockCount);
    }

    public void convert(PageBuilder pageBuilder)
    {
        try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
            System.out.println("declarePositions :" + root.getRowCount());
            pageBuilder.declarePositions(root.getRowCount());
            for (int column = 0; column < columns.size(); column++) {
                ArrowColumnHandle arrowColumnHandle = columns.get(column);
                System.out.println("Data :" + root.contentToTSVString() + "\n" + "declarePositions :" + root.getRowCount());
                System.out.println("partitionValues :" + partitionValues);
                System.out.println("Adding Key :" + arrowColumnHandle.getColumnName());
                System.out.println("isPartitionKey :" + arrowColumnHandle.isPartitionKey());
                if (arrowColumnHandle.isPartitionKey()) {
                    convertType(pageBuilder.getBlockBuilder(column),
                            arrowColumnHandle.getColumnType(),
                            partitionValues.get(arrowColumnHandle.getColumnName()),
                            root.getRowCount());
                }
                else {
                    convertType(pageBuilder.getBlockBuilder(column),
                            columns.get(column).getColumnType(),
                            root.getVector(column),
                            0,
                            root.getVector(column).getValueCount());
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void convertType(BlockBuilder output, Type type, Object value, int declaredPositions)
    {
        System.out.println("Adding Partition Value with value " + value);
        Class<?> javaType = type.getJavaType();
        System.out.println("javaType: " + javaType);
        System.out.println("type.getTypeId().getId(): " + type.getTypeId().getId());
        for (int i = 0; i < declaredPositions; i++) {
            try {
                if (javaType == boolean.class) {
                    type.writeBoolean(output, (Boolean) value);
                }
                else if (javaType == long.class) {
                    if (type.equals(BIGINT)) {
                        type.writeLong(output, Long.parseLong(value.toString()));
                    }
                    else if (type.equals(INTEGER)) {
                        type.writeLong(output, Long.parseLong(value.toString()));
                    }
                    else if (type.equals(DATE)) {
                        type.writeLong(output, ((Date) value).getTime());
                    }
                    else if (type.equals(TIMESTAMP_MICROS)) {
                        type.writeLong(output, ((Timestamp) value).getTime());
                    }
                    else if (type.equals(TIME_MICROS)) {
                        type.writeLong(output, ((Timestamp) value).getTime());
                    }
                }
                else if (javaType == double.class) {
                    type.writeDouble(output, (Double) value);
                }
                else if (javaType == Slice.class) {
                    type.writeSlice(output, wrappedBuffer(String.valueOf(value).getBytes(StandardCharsets.UTF_8)));
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
                }
            }
            catch (ClassCastException ex) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type), ex);
            }
        }
    }

    private void convertType(BlockBuilder output, Type type, FieldVector vector, int offset, int length)
    {
        Class<?> javaType = type.getJavaType();
        System.out.println("SPI Type " + type);
        System.out.println("javaType " + javaType);
        try {
            if (javaType == boolean.class) {
                writeVectorValues(output, vector, index -> type.writeBoolean(output, ((BitVector) vector).get(index) == 1), offset, length);
            }
            else if (javaType == long.class) {
                if (type.equals(BIGINT)) {
                    writeVectorValues(output, vector, index -> type.writeLong(output, ((BigIntVector) vector).get(index)), offset, length);
                }
                else if (type.equals(INTEGER)) {
                    writeVectorValues(output, vector, index -> type.writeLong(output, ((IntVector) vector).get(index)), offset, length);
                }
                else if (type instanceof DecimalType decimalType) {
                    writeVectorValues(output, vector, index -> writeObjectShortDecimal(output, decimalType, vector, index), offset, length);
                }
                else if (type.equals(DATE)) {
                    writeVectorValues(output, vector, index -> type.writeLong(output, ((DateDayVector) vector).get(index)), offset, length);
                }
                else if (type.equals(TIMESTAMP_MICROS)) {
                    writeVectorValues(output, vector, index -> type.writeLong(output, ((TimeStampVector) vector).get(index)), offset, length);
                }
                else if (type.equals(TIME_MICROS)) {
                    writeVectorValues(output, vector, index -> type.writeLong(output, ((TimeMicroVector) vector).get(index) * PICOSECONDS_PER_MICROSECOND), offset, length);
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
                }
            }
            else if (javaType == double.class) {
                writeVectorValues(output, vector, index -> type.writeDouble(output, ((Float8Vector) vector).get(index)), offset, length);
            }
            else if (type.getJavaType() == Int128.class) {
                writeVectorValues(output, vector, index -> writeObjectLongDecimal(output, type, vector, index), offset, length);
            }
            else if (javaType == Slice.class) {
                writeVectorValues(output, vector, index -> writeSlice(output, type, vector, index), offset, length);
            }
            else if (javaType == LongTimestampWithTimeZone.class) {
                writeVectorValues(output, vector, index -> writeObjectTimestampWithTimezone(output, type, vector, index), offset, length);
            }
            else if (javaType == Block.class) {
                writeVectorValues(output, vector, index -> writeBlock(output, type, vector, index), offset, length);
            }
            else if (javaType == LongTimestamp.class) {
                writeVectorValues(output, vector, index -> writeTimestampNanoWithTimezone(output, type, vector, index), offset, length);
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
            }
        }
        catch (ClassCastException ex) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type), ex);
        }
    }

    private void writeVectorValues(BlockBuilder output, FieldVector vector, Consumer<Integer> consumer, int offset, int length)
    {
        for (int i = offset; i < offset + length; i++) {
            if (vector.isNull(i)) {
                output.appendNull();
            }
            else {
                consumer.accept(i);
            }
        }
    }

    private void writeSlice(BlockBuilder output, Type type, FieldVector vector, int index)
    {
        if (type instanceof VarcharType) {
            byte[] slice = ((VarCharVector) vector).get(index);
            type.writeSlice(output, wrappedBuffer(slice));
        }
        else if (type instanceof VarbinaryType) {
            byte[] slice = ((VarBinaryVector) vector).get(index);
            type.writeSlice(output, wrappedBuffer(slice));
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Slice: " + type.getTypeSignature());
        }
    }

    private void writeObjectLongDecimal(BlockBuilder output, Type type, FieldVector vector, int index)
    {
        if (type instanceof DecimalType decimalType) {
            verify(!decimalType.isShort(), "The type should be long decimal");
            BigDecimal decimal = vector.getMinorType() == DECIMAL256 ? ((Decimal256Vector) vector).getObject(index) : ((DecimalVector) vector).getObject(index);
            type.writeObject(output, Decimals.encodeScaledValue(decimal, decimalType.getScale()));
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Object: " + type.getTypeSignature());
        }
    }

    private void writeObjectShortDecimal(BlockBuilder output, DecimalType decimalType, FieldVector vector, int index)
    {
        verify(decimalType.isShort(), "The type should be short decimal");
        BigDecimal decimal = vector.getMinorType() == DECIMAL256 ? ((Decimal256Vector) vector).getObject(index) : ((DecimalVector) vector).getObject(index);
        decimalType.writeLong(output, encodeShortScaledValue(decimal, decimalType.getScale()));
    }

    private void writeObjectTimestampWithTimezone(BlockBuilder output, Type type, FieldVector vector, int index)
    {
        verify(type.equals(TIMESTAMP_TZ_MICROS));
        long epochMicros = ((TimeStampVector) vector).get(index);
        int picosOfMillis = toIntExact(floorMod(epochMicros, MICROSECONDS_PER_MILLISECOND)) * PICOSECONDS_PER_MICROSECOND;
        // BigQuery's TIMESTAMP type represents an instant in time so always uses UTC as the zone - the original zone of the input literal is lost
        type.writeObject(output, fromEpochMillisAndFraction(floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND), picosOfMillis, UTC_KEY));
    }

    private void writeTimestampNanoWithTimezone(BlockBuilder output, Type type, FieldVector vector, int index)
    {
        long epochMicros = ((TimeStampNanoVector) vector).get(index) / 1000;
        int picosOfNanos = toIntExact(floorMod(epochMicros, NANOSECONDS_PER_MILLISECOND)) * PICOSECONDS_PER_NANOSECOND;
        type.writeObject(output, new LongTimestamp(epochMicros, picosOfNanos));
    }

    private void writeBlock(BlockBuilder output, Type type, FieldVector vector, int index)
    {
        if (type instanceof ArrayType && vector.getMinorType() == LIST) {
            writeArrayBlock(output, type, vector, index);
            return;
        }
        if (type instanceof RowType && vector.getMinorType() == STRUCT) {
            writeRowBlock(output, type, vector, index);
            return;
        }
        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Block: " + type.getTypeSignature());
    }

    private void writeArrayBlock(BlockBuilder output, Type type, FieldVector vector, int index)
    {
        BlockBuilder block = output.beginBlockEntry();
        Type elementType = getOnlyElement(type.getTypeParameters());

        ArrowBuf offsetBuffer = vector.getOffsetBuffer();

        int start = offsetBuffer.getInt((long) index * OFFSET_WIDTH);
        int end = offsetBuffer.getInt((long) (index + 1) * OFFSET_WIDTH);

        FieldVector innerVector = ((ListVector) vector).getDataVector();

        TransferPair transferPair = innerVector.getTransferPair(allocator);
        transferPair.splitAndTransfer(start, end - start);
        try (FieldVector sliced = (FieldVector) transferPair.getTo()) {
            convertType(block, elementType, sliced, 0, sliced.getValueCount());
        }
        output.closeEntry();
    }

    private void writeRowBlock(BlockBuilder output, Type type, FieldVector vector, int index)
    {
        BlockBuilder builder = output.beginBlockEntry();
        ImmutableList.Builder<String> fieldNamesBuilder = ImmutableList.builder();
        for (int i = 0; i < type.getTypeSignature().getParameters().size(); i++) {
            TypeSignatureParameter parameter = type.getTypeSignature().getParameters().get(i);
            fieldNamesBuilder.add(parameter.getNamedTypeSignature().getName().orElse("field" + i));
        }
        List<String> fieldNames = fieldNamesBuilder.build();
        checkState(fieldNames.size() == type.getTypeParameters().size(), "fieldNames size differs from type %s type parameters size", type);

        for (int i = 0; i < type.getTypeParameters().size(); i++) {
            FieldVector innerVector = ((StructVector) vector).getChild(fieldNames.get(i));
            convertType(builder, type.getTypeParameters().get(i), innerVector, index, 1);
        }
        output.closeEntry();
    }

    @Override
    public long getCompletedBytes()
    {
        return readBytes.get();
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        System.out.println("isFinished?: " + this.readBlocks.get());
        return this.readBlocks.get() >= this.blockCount;
    }

    @Override
    public Page getNextPage()
    {
        try {
            ArrowBlock block = this.arrowBlocks.get(this.readBlocks.get());
            reader.loadRecordBatch(block);
            convert(pageBuilder);
            readBytes.addAndGet(block.getBodyLength());
            this.readBlocks.addAndGet(1);
            return pageBuilder.build();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getMemoryUsage()
    {
        return readBytes.get();
    }

    @Override
    public void close()
    {
        allocator.close();
    }
}
