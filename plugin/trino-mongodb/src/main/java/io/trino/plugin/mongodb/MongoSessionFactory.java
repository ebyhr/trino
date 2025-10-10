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
package io.trino.plugin.mongodb;

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.mongo.v3_1.MongoTelemetry;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeManager;

import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;

public class MongoSessionFactory
{
    private final TypeManager typeManager;
    private final OpenTelemetry openTelemetry;
    private final Set<MongoClientSettingConfigurator> configurators;
    private final String schemaCollection;
    private final boolean caseInsensitiveNameMatching;
    private final int cursorBatchSize;
    private final String implicitPrefix;
    private final Cache<SchemaTableName, MongoTable> tableCache;

    @Inject
    public MongoSessionFactory(
            TypeManager typeManager,
            OpenTelemetry openTelemetry,
            Set<MongoClientSettingConfigurator> configurators,
            MongoClientConfig config)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.openTelemetry = requireNonNull(openTelemetry, "openTelemetry is null");
        this.configurators = ImmutableSet.copyOf(configurators);
        this.schemaCollection = config.getSchemaCollection();
        this.caseInsensitiveNameMatching = config.isCaseInsensitiveNameMatching();
        this.cursorBatchSize = config.getCursorBatchSize();
        this.implicitPrefix = config.getImplicitRowFieldPrefix();
        this.tableCache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(1, MINUTES)  // TODO: Configure
                .build();
    }

    public MongoSession create()
    {
        // Lazily build MongoClientSettings because MongoClientSettings.Builder.applyConnectionString looks up DNS record when SRV is used
        MongoClientSettings.Builder options = MongoClientSettings.builder();
        configurators.forEach(configurator -> configurator.configure(options));
        options.addCommandListener(MongoTelemetry.builder(openTelemetry).build().newCommandListener());
        MongoClient client = MongoClients.create(options.build());

        return new MongoSession(
                typeManager,
                client,
                schemaCollection,
                caseInsensitiveNameMatching,
                cursorBatchSize,
                tableCache,
                implicitPrefix);
    }
}
