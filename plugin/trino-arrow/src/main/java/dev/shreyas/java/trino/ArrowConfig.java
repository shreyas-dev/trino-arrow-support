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
package dev.shreyas.java.trino;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

public class ArrowConfig
{
    private String uri = "file:/opt/";
    private int batchSize = 32768;
    private String connectionURL = "jdbc:mysql://host.docker.internal:3306/hive_metastore?createDatabaseIfNotExist=true";
    private String connectionDriverName = "com.mysql.cj.jdbc.Driver";
    private String connectionUserName = "hiveuser";
    private String connectionPassword = "hivepassword";
    private String databaseName = "hive_metastore";
    private boolean dockerRun = true;

    @Config("arrow.read.batch-size")
    @ConfigDescription("Batch size of arrow read")
    public ArrowConfig setBatchSize(int batchSize)
    {
        this.batchSize = batchSize;
        return this;
    }

    @Config("arrow.read.uri")
    @ConfigDescription("file path to read parquet")
    public ArrowConfig setUri(String uri)
    {
        this.uri = uri;
        return this;
    }

    @Config("arrow.hive.connectionURL")
    @ConfigDescription("connection URL for hive")
    public ArrowConfig setConnectionURL(String connectionURL)
    {
        this.connectionURL = connectionURL;
        return this;
    }

    @Config("arrow.hive.connectionDriverName")
    @ConfigDescription("connectionDriverName for hive")
    public ArrowConfig setConnectionDriverName(String connectionDriverName)
    {
        this.connectionDriverName = connectionDriverName;
        return this;
    }

    @Config("arrow.hive.connectionUserName")
    @ConfigDescription("connectionUserName for hive")
    public ArrowConfig setConnectionUserName(String connectionUserName)
    {
        this.connectionUserName = connectionUserName;
        return this;
    }

    @Config("arrow.hive.connectionPassword")
    @ConfigDescription("connectionPassword for hive")
    public ArrowConfig setConnectionPassword(String connectionPassword)
    {
        this.connectionPassword = connectionPassword;
        return this;
    }

    @Config("arrow.hive.databaseName")
    @ConfigDescription("connectionPassword for hive")
    public ArrowConfig setDatabaseName(String databaseName)
    {
        this.databaseName = databaseName;
        return this;
    }

    @Config("arrow.hive.docker.run")
    @ConfigDescription("connectionPassword for hive")
    public ArrowConfig setDockerRun(boolean dockerRun)
    {
        this.dockerRun = dockerRun;
        return this;
    }

    public String getUri()
    {
        return uri;
    }

    public int getBatchSize()
    {
        return batchSize;
    }

    public String getConnectionURL()
    {
        return connectionURL;
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public String getConnectionDriverName()
    {
        return connectionDriverName;
    }

    public String getConnectionUserName()
    {
        return connectionUserName;
    }

    public String getConnectionPassword()
    {
        return connectionPassword;
    }

    public boolean isDockerRun()
    {
        return dockerRun;
    }
}
