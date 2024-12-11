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
package io.trino.plugin.iceberg.parquet;

import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.parquet.TypeToMessageType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import static org.apache.iceberg.types.Type.TypeID.TIMESTAMP_NANO;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

public class IcebergTypeToMessageType
        extends TypeToMessageType
{
    private static final LogicalTypeAnnotation TIMESTAMP_NANOS = LogicalTypeAnnotation.timestampType(false /* not adjusted to UTC */, LogicalTypeAnnotation.TimeUnit.NANOS);
    private static final LogicalTypeAnnotation TIMESTAMPTZ_NANOS = LogicalTypeAnnotation.timestampType(true /* adjusted to UTC */, LogicalTypeAnnotation.TimeUnit.NANOS);

    @Override
    public MessageType convert(Schema schema, String name)
    {
        Types.MessageTypeBuilder builder = Types.buildMessage();

        for (org.apache.iceberg.types.Types.NestedField field : schema.columns()) {
            builder.addField(field(field));
        }

        return builder.named(AvroSchemaUtil.makeCompatibleName(name));
    }

    @Override
    public Type primitive(org.apache.iceberg.types.Type.PrimitiveType primitive, Type.Repetition repetition, int id, String originalName)
    {
        String name = AvroSchemaUtil.makeCompatibleName(originalName);
        if (primitive.typeId() == TIMESTAMP_NANO) {
            if (((org.apache.iceberg.types.Types.TimestampNanoType) primitive).shouldAdjustToUTC()) {
                return Types.primitive(INT64, repetition).as(TIMESTAMPTZ_NANOS).id(id).named(name);
            }
            return Types.primitive(INT64, repetition).as(TIMESTAMP_NANOS).id(id).named(name);
        }
        return super.primitive(primitive, repetition, id, originalName);
    }
}
