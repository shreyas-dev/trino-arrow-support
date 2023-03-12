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
package dev.shreyas.java.trino.client;

import com.google.inject.Inject;
import dev.shreyas.java.trino.ArrowConfig;
import dev.shreyas.java.trino.column.PartCol;
import dev.shreyas.java.trino.split.IpcArrowSplit;
import dev.shreyas.java.trino.table.ArrowTableHandle;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;

public class ArrowClient
{
    private final ArrowConfig arrowConfig;

    @Inject
    public ArrowClient(ArrowConfig arrowConfig)
    {
        this.arrowConfig = arrowConfig;
    }

    public List<ColumnMetadata> getColumnsMetadata()
    {
        System.out.println(this.arrowConfig.getUri());
        List<ColumnMetadata> columnMetadata = new ArrayList<>();
        columnMetadata.add(new ColumnMetadata("registration_dttm", TimestampType.TIMESTAMP_NANOS));
        columnMetadata.add(new ColumnMetadata("id", IntegerType.INTEGER));
        columnMetadata.add(new ColumnMetadata("first_name", VarcharType.createUnboundedVarcharType()));
        columnMetadata.add(new ColumnMetadata("last_name", VarcharType.createUnboundedVarcharType()));
        columnMetadata.add(new ColumnMetadata("email", VarcharType.createUnboundedVarcharType()));
        columnMetadata.add(new ColumnMetadata("gender", VarcharType.createUnboundedVarcharType()));
        columnMetadata.add(new ColumnMetadata("ip_address", VarcharType.createUnboundedVarcharType()));
        columnMetadata.add(new ColumnMetadata("cc", VarcharType.createUnboundedVarcharType()));
        columnMetadata.add(new ColumnMetadata("country", VarcharType.createUnboundedVarcharType()));
        columnMetadata.add(new ColumnMetadata("birthdate", VarcharType.createUnboundedVarcharType()));
        columnMetadata.add(new ColumnMetadata("salary", DoubleType.DOUBLE));
        columnMetadata.add(new ColumnMetadata("title", VarcharType.createUnboundedVarcharType()));
        columnMetadata.add(new ColumnMetadata("comments", VarcharType.createUnboundedVarcharType()));
        return columnMetadata;
    }

//    public List<ArrowSplit> getArrowSplits(ArrowTableHandle tableHandle, HostAddress hostAddress)
//    {
//        String finalPath = this.arrowConfig.getUri() + File.separator + tableHandle.getSchemaName() + File.separator + tableHandle.getTableName() + File.separator;
//        System.out.println(finalPath);
//        List<ArrowSplit> splits = new ArrayList<>();
//
//        ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
//        try (
//                BufferAllocator allocator = new RootAllocator();
//                DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, finalPath);
//                Dataset dataset = datasetFactory.finish();
//                Scanner scanner = dataset.newScan(options);
//                ArrowReader reader = scanner.scanBatches();) {
//            while (reader.loadNextBatch()) {
//                try (VectorSchemaRoot vectorSchemaRoot = reader.getVectorSchemaRoot()) {
//                    VectorUnloader unloader = new VectorUnloader(vectorSchemaRoot);
//                    ArrowRecordBatch recordBatch = unloader.getRecordBatch();
//                    splits.add(new ArrowSplit(hostAddress));
//                    recordBatch.close();
//                }
//            }
//            Collections.shuffle(splits);
//        }
//        catch (Exception e) {
//            e.printStackTrace();
//        }
//        return splits;
//    }

    // System A  arrow -> System B arrow ->
    public List<IpcArrowSplit> writeAsArrowFileWriter(ArrowTableHandle tableHandle, PartCol partCol, List<HostAddress> hostAddress)
    {
        String finalPath = this.arrowConfig.getUri() + File.separator + tableHandle.getSchemaName() + File.separator + tableHandle.getTableName() + File.separator;
        System.out.println(finalPath);
        List<IpcArrowSplit> arrowSplits = new ArrayList<>();
        System.out.println("HDFS path :" + partCol.getLocation());
        ScanOptions options = new ScanOptions(/*batchSize*/ this.arrowConfig.getBatchSize());
        try (
                BufferAllocator allocator = new RootAllocator();
                DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, partCol.getLocation());
                Dataset dataset = datasetFactory.finish();
                Scanner scanner = dataset.newScan(options);
                ArrowReader reader = scanner.scanBatches();) {
            while (reader.loadNextBatch()) {
                try (VectorSchemaRoot vectorSchemaRoot = reader.getVectorSchemaRoot()) {
                    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                    ArrowFileWriter writer = new ArrowFileWriter(vectorSchemaRoot, /*DictionaryProvider=*/null, Channels.newChannel(byteArrayOutputStream));
                    writer.start();
                    writer.writeBatch();
                    writer.end();
                    arrowSplits.add(new IpcArrowSplit(byteArrayOutputStream.toByteArray(), hostAddress, partCol.getPartKeyValues()));
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("No. of Arrow Splits: " + arrowSplits.size());
        return arrowSplits;
    }
}
