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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.hive.HiveQueryRunner.copyTpchTablesBucketed;
import static io.trino.plugin.hive.HiveQueryRunner.createBucketedSession;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.security.SelectedRole.Type.ROLE;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;

public final class HadoopQueryRunner
{
    public static final String HIVE_CATALOG = "hive";
    private static final String HIVE_BUCKETED_CATALOG = "hive_bucketed";
    private static final String TPCH_SCHEMA = "tpch";
    private static final SelectedRole ADMIN_ROLE = new SelectedRole(ROLE, Optional.of("admin"));

    private HadoopQueryRunner() {}

    static {
        System.setProperty("HADOOP_USER_NAME", "hive");
    }

    public static DistributedQueryRunner createHadoopQueryRunner(
            TestingHadoopServer server,
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(createSession())
                .setExtraProperties(extraProperties)
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.installPlugin(new TestingHivePlugin());
            queryRunner.createCatalog("tpch", "tpch");

            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("hive.metastore.uri", server.getThriftUri());
            connectorProperties.putIfAbsent("hive.metastore.thrift.client.socks-proxy", server.getSocksProxy());
            connectorProperties.putIfAbsent("hive.hdfs.socks-proxy", server.getSocksProxy());
            connectorProperties.putIfAbsent("hive.max-partitions-per-scan", "1000");
            connectorProperties.putIfAbsent("hive.security", "sql-standard");
            connectorProperties.putIfAbsent("hive.translate-hive-views", "true");

            Map<String, String> hiveBucketedProperties = ImmutableMap.<String, String>builder()
                    .putAll(connectorProperties)
                    .put("hive.max-initial-split-size", "10kB") // so that each bucket has multiple splits
                    .put("hive.max-split-size", "10kB") // so that each bucket has multiple splits
                    .put("hive.storage-format", "TEXTFILE") // so that there's no minimum split size for the file
                    .put("hive.compression-codec", "NONE") // so that the file is splittable
                    .build();
            queryRunner.createCatalog(HIVE_CATALOG, "hive", connectorProperties);
            queryRunner.createCatalog(HIVE_BUCKETED_CATALOG, "hive", hiveBucketedProperties);

            queryRunner.execute(createSession(), "CREATE SCHEMA tpch");
            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), tables);
            queryRunner.execute(createSession(), "CREATE SCHEMA tpch_bucketed");
            copyTpchTablesBucketed(queryRunner, "tpch", TINY_SCHEMA_NAME, createBucketedSession(Optional.empty()), tables);
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setIdentity(Identity.forUser("hive")
                        .withRoles(ImmutableMap.of(
                                "hive", ADMIN_ROLE,
                                "tpch", ADMIN_ROLE))
                        .build())
                .setCatalog("hive")
                .setSchema(TPCH_SCHEMA)
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        // Set "--user hive" to your CLI and execute "SET ROLE admin" for queries to work
        Logging.initialize();
        DistributedQueryRunner queryRunner = createHadoopQueryRunner(
                new TestingHadoopServer(),
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of(),
                TpchTable.getTables());
        Thread.sleep(10);
        Logger log = Logger.get(HadoopQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
