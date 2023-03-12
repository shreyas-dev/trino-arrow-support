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
import dev.shreyas.java.trino.column.ArrowColumnHandle;
import dev.shreyas.java.trino.column.PartCol;
import dev.shreyas.java.trino.table.ArrowTableHandle;
import dev.shreyas.java.trino.util.HiveUtils;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HiveClient
{
    private final ArrowConfig arrowConfig;
    private final Connection hiveMetaStoreClient;

    @Inject
    public HiveClient(ArrowConfig arrowConfig) throws ClassNotFoundException, SQLException
    {
        this.arrowConfig = arrowConfig;
        this.hiveMetaStoreClient = connectToHive();
    }

    private Connection connectToHive() throws ClassNotFoundException, SQLException
    {
        Class.forName(arrowConfig.getConnectionDriverName());
        Connection con = DriverManager.getConnection(arrowConfig.getConnectionURL(), arrowConfig.getConnectionUserName(), arrowConfig.getConnectionPassword());
        Statement statement = con.createStatement();
        System.out.println("Connected to Hive Metastore");
        return con;
    }

    public List<SchemaTableName> getTables(String dbName)
    {
        final String showDatabasesQuery = "SELECT TBL_NAME FROM TBLS INNER JOIN DBS ON DBS.DB_ID = TBLS.DB_ID WHERE DBS.NAME = '" + dbName + "';";
        System.out.println(showDatabasesQuery);
        List<SchemaTableName> hiveDatabases = new ArrayList<>();
        try {
            Statement showDatabasesStmt = hiveMetaStoreClient.createStatement();
            ResultSet resultSet = showDatabasesStmt.executeQuery(showDatabasesQuery);
            while (resultSet.next()) {
                hiveDatabases.add(new SchemaTableName(dbName, resultSet.getString(1)));
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return hiveDatabases;
    }

    public List<String> getHiveDatabases()
    {
        final String showDatabasesQuery = "SELECT NAME from DBS;";
        List<String> hiveDatabases = new ArrayList<>();
        try {
            System.out.println(hiveMetaStoreClient.getClientInfo().toString());
            Statement showDatabasesStmt = hiveMetaStoreClient.createStatement();
            ResultSet resultSet = showDatabasesStmt.executeQuery(showDatabasesQuery);
            while (resultSet.next()) {
                hiveDatabases.add(resultSet.getString(1));
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return hiveDatabases;
    }

    public ArrowTableHandle getArrowTableHandle(SchemaTableName tableName)
    {
        final String showTableQuery = "SELECT TBLS.DB_ID,TBLS.TBL_ID,TBLS.SD_ID,CDS.CD_ID FROM TBLS" +
                " INNER JOIN DBS ON DBS.DB_ID = TBLS.DB_ID" +
                " INNER JOIN SDS ON SDS.SD_ID = TBLS.SD_ID" +
                " INNER JOIN CDS ON SDS.CD_ID = CDS.CD_ID" +
                " " +
                " WHERE" +
                " DBS.NAME = '" + tableName.getSchemaName() + "'" +
                " AND TBLS.TBL_NAME = '" + tableName.getTableName() + "'";
        try {
            System.out.println("Here getArrowTableHandle");
            System.out.println(showTableQuery);
            Statement showDatabasesStmt = hiveMetaStoreClient.createStatement();
            ResultSet resultSet = showDatabasesStmt.executeQuery(showTableQuery);
            if (resultSet.next()) {
                return new ArrowTableHandle(resultSet.getLong(1), tableName.getSchemaName(), resultSet.getLong(2), tableName.getTableName(), resultSet.getLong(3), resultSet.getLong(4));
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public ConnectorTableMetadata getConnectorTableMetadata(ArrowTableHandle tableHandle)
    {
        List<ArrowColumnHandle> cols = getTableColumns(tableHandle);
        cols.addAll(getTablePartitions(tableHandle));
        List<ColumnMetadata> columnMetadataList = cols.stream()
                .map(ArrowColumnHandle::getColumnMetadata)
                .toList();
        return new ConnectorTableMetadata(tableHandle.toSchemaTableName(), columnMetadataList);
    }

    private List<ArrowColumnHandle> getTableColumns(ArrowTableHandle arrowTableHandle)
    {
        final String showColumnQuery = "SELECT CD_ID,COLUMN_NAME,TYPE_NAME,INTEGER_IDX FROM COLUMNS_V2" +
                " WHERE CD_ID = " + arrowTableHandle.getCdId() +
                " ORDER BY INTEGER_IDX ASC";
        List<ArrowColumnHandle> arrowColumnHandles = new ArrayList<>();
        System.out.println(showColumnQuery);
        try (Statement showDatabasesStmt = hiveMetaStoreClient.createStatement()) {
            ResultSet resultSet = showDatabasesStmt.executeQuery(showColumnQuery);
            while (resultSet.next()) {
                arrowColumnHandles.add(new ArrowColumnHandle(resultSet.getLong(1), resultSet.getString(2), HiveUtils.getTrinoTypeFromHiveType(resultSet.getString(3)), resultSet.getInt(4), false));
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return arrowColumnHandles;
    }

    private List<ArrowColumnHandle> getTablePartitions(ArrowTableHandle arrowTableHandle)
    {
        final String showPartitionColsQuery = "SELECT TBL_ID,PKEY_NAME,PKEY_TYPE,INTEGER_IDX FROM PARTITION_KEYS" +
                " WHERE TBL_ID = " + arrowTableHandle.getTableId() +
                " ORDER BY INTEGER_IDX ASC";
        List<ArrowColumnHandle> arrowColumnHandles = new ArrayList<>();
        System.out.println(showPartitionColsQuery);
        try (Statement showDatabasesStmt = hiveMetaStoreClient.createStatement()) {
            ResultSet resultSet = showDatabasesStmt.executeQuery(showPartitionColsQuery);
            while (resultSet.next()) {
                arrowColumnHandles.add(new ArrowColumnHandle(resultSet.getLong(1), resultSet.getString(2), HiveUtils.getTrinoTypeFromHiveType(resultSet.getString(3)), resultSet.getInt(4), true));
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return arrowColumnHandles;
    }

    public Map<String, ColumnHandle> getColumnHandles(ArrowTableHandle arrowTableHandle)
    {
        List<ArrowColumnHandle> cols = getTableColumns(arrowTableHandle);
        cols.addAll(getTablePartitions(arrowTableHandle));
        return cols.stream().collect(Collectors.toMap(ArrowColumnHandle::getColumnName, (arrowColumnHandle -> arrowColumnHandle)));
    }

    private String getReadLocation(String hdfsPath)
    {
        if (arrowConfig.isDockerRun()) {
            return hdfsPath.replace(arrowConfig.getHdfsWarehousePath(), arrowConfig.getUri());
        }
        return hdfsPath;
    }

    public List<PartCol> getPartitions(ArrowTableHandle arrowTableHandle)
    {
        Map<Long, PartCol> partitionMap = new HashMap<>();
        System.out.println("arrowTableHandle Query :" + arrowTableHandle.getPartitionQuery());
        if (arrowTableHandle.getPartitionQuery() == null || arrowTableHandle.getPartitionQuery().isEmpty()) {
            final String hasPartitionQuery = "SELECT COUNT(1) FROM PARTITIONS WHERE TBL_ID = " + arrowTableHandle.getTableId();
            try (Statement hasPartitionQueryStmt = hiveMetaStoreClient.createStatement()) {
                System.out.println("Executing has partitions query :" + hasPartitionQuery);
                ResultSet resultSet = hasPartitionQueryStmt.executeQuery(hasPartitionQuery);
                resultSet.next();
                if (resultSet.getLong(1) > 0) {
                    final String getPartitionQuery = "SELECT PKV.PART_ID,S.LOCATION,PK.PKEY_NAME,PKV.PART_KEY_VAL FROM PARTITION_KEY_VALS AS PKV" +
                            " INNER JOIN PARTITIONS AS P ON PKV.PART_ID = P.PART_ID" +
                            " INNER JOIN SDS AS S ON S.SD_ID = P.SD_ID" +
                            " INNER JOIN PARTITION_KEYS AS PK ON PK.TBL_ID = P.TBL_ID AND PK.INTEGER_IDX = PKV.INTEGER_IDX" +
                            " WHERE P.TBL_ID=" + arrowTableHandle.getTableId();
                    try (Statement getPartitionStmt = hiveMetaStoreClient.createStatement()) {
                        System.out.println("getPartitionQuery :" + getPartitionQuery);
                        ResultSet resultSetForPartitions = getPartitionStmt.executeQuery(getPartitionQuery);
                        while (resultSetForPartitions.next()) {
                            long partId = resultSetForPartitions.getLong(1);
                            String location = getReadLocation(resultSetForPartitions.getString(2));
                            PartCol partCol = partitionMap.getOrDefault(partId, new PartCol(partId, location));
                            partCol.addPartKeyValue(resultSetForPartitions.getString(3), resultSetForPartitions.getObject(4));
                            partitionMap.put(partId, partCol);
                        }
                    }
                }
                else {
                    final String getNonPartitionedQuery = "SELECT SDS.LOCATION FROM SDS" +
                            " WHERE SDS.SD_ID = " + arrowTableHandle.getSdId();
                    System.out.println("Get Non Partition Query :" + getNonPartitionedQuery);
                    try (Statement getNonPartitionedStmt = hiveMetaStoreClient.createStatement()) {
                        ResultSet resultSetForNonPartitioned = getNonPartitionedStmt.executeQuery(getNonPartitionedQuery);
                        resultSetForNonPartitioned.next();
                        String location = getReadLocation(resultSetForNonPartitioned.getString(1));
                        location = getReadLocation(location);
                        PartCol partCol = partitionMap.getOrDefault(-1L, new PartCol(-1L, location));
                        partitionMap.put(-1L, partCol);
                    }
                }
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        else {
            final String getPartitionQuery = "SELECT P.PART_ID,S.LOCATION,PK.PKEY_NAME,PKV.PART_KEY_VAL FROM PARTITION_KEY_VALS AS PKV" +
                    " INNER JOIN PARTITIONS AS P ON PKV.PART_ID = P.PART_ID" +
                    " INNER JOIN SDS AS S ON S.SD_ID = P.SD_ID" +
                    " INNER JOIN PARTITION_KEYS AS PK ON PK.TBL_ID = P.TBL_ID AND PK.INTEGER_IDX = PKV.INTEGER_IDX" +
                    " WHERE P.TBL_ID=" + arrowTableHandle.getTableId() +
                    " AND PKV.PART_ID IN ( " + arrowTableHandle.getPartitionQuery() + " )";
            System.out.println("Get Partition Query :" + getPartitionQuery);
            try (Statement getPartitionStmt = hiveMetaStoreClient.createStatement()) {
                ResultSet resultSet = getPartitionStmt.executeQuery(getPartitionQuery);
                while (resultSet.next()) {
                    long partId = resultSet.getLong(1);
                    String location = getReadLocation(resultSet.getString(2));
                    PartCol partCol = partitionMap.getOrDefault(partId, new PartCol(partId, location));
                    partCol.addPartKeyValue(resultSet.getString(3), resultSet.getString(4));
                    partitionMap.put(partId, partCol);
                    System.out.println("PartCol : " + partCol.getPartKeyValues());
                }
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return partitionMap.values().stream().toList();
    }
}
