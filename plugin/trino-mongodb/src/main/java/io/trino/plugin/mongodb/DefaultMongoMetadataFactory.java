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

import com.google.inject.Inject;

import static java.util.Objects.requireNonNull;

public class DefaultMongoMetadataFactory
        implements MongoMetadataFactory
{
    private final MongoSessionFactory sessionFactory;

    @Inject
    public DefaultMongoMetadataFactory(MongoSessionFactory sessionFactory)
    {
        this.sessionFactory = requireNonNull(sessionFactory, "sessionFactory is null");
    }

    @Override
    public MongoMetadata create()
    {
        // TODO Create and close a new session in MongoMetadata
        return new MongoMetadata(sessionFactory.create());
    }
}
