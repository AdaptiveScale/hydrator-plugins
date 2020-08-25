/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.transform;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Transforms flat records into hierarchical records.
 */
@Plugin(type = "transform")
@Name("CreateRecord")
@Description("Create Record plugin transforms flat structures into hierarchical structures.")
public class CreateRecordTransform extends Transform<StructuredRecord, StructuredRecord> {

  /**
   * Create hierarchy config
   */
  public static class CreateRecordTransformConfig extends PluginConfig {
    public static final String FIELD_MAPPING = "fieldMapping";
    public static final String INCLUDE_NON_MAPPED_FIELDS = "includeNonMappedFields";

    @Description("Specifies the mapping for generating the hierarchy.")
    @Name(FIELD_MAPPING)
    String fieldMapping;

    @Description("Specifies whether the fields in the input schema that are not part of the mapping, " +
      "should be carried over as-is.")
    @Name(INCLUDE_NON_MAPPED_FIELDS)
    String includeNonMappedFields;

    public CreateRecordTransformConfig(String fieldMapping, String includeNonMappedFields) {
      this.fieldMapping = fieldMapping;
      this.includeNonMappedFields = includeNonMappedFields;
    }

    public String getFieldMapping() {
      return fieldMapping;
    }

    public JsonElement getFieldMappingJson() {
      return GSON.fromJson(fieldMapping, JsonElement.class);
//      return GSON.toJsonTree(fieldMapping);
    }

    public String getIncludeNonMappedFields() {
      return includeNonMappedFields;
    }

    public void validate(FailureCollector collector) {
      if (!containsMacro(FIELD_MAPPING)) {
        try {
          getFieldMappingJson();
        } catch (Exception e) {
          collector.addFailure("Invalid field mapping provided.",
                               "Please provide valid field mapping.").withConfigProperty(FIELD_MAPPING);
        }
      }
      if (!containsMacro(INCLUDE_NON_MAPPED_FIELDS) && Strings.isNullOrEmpty(INCLUDE_NON_MAPPED_FIELDS)) {
        collector.addFailure("Include missing fields property missing.",
                             "Please provide include missing fields value.")
          .withConfigProperty(INCLUDE_NON_MAPPED_FIELDS);
      }
      collector.getOrThrowException();
    }
  }

  private final CreateRecordTransformConfig createRecordTransformConfig;
  // cache input schema hash to output schema so we don't have to build it each time
  private Map<Schema, Schema> schemaCache = Maps.newHashMap();
  private static final Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(CreateRecordTransform.class);

  public CreateRecordTransform(CreateRecordTransformConfig createRecordTransformConfig) {
    this.createRecordTransformConfig = createRecordTransformConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    createRecordTransformConfig.validate(failureCollector);
    Schema outputSchema = null;
    if (pipelineConfigurer.getStageConfigurer().getInputSchema() != null) {
      //validate the input schema and get the output schema for it
      outputSchema = getOutputSchema(pipelineConfigurer.getStageConfigurer().getInputSchema(), failureCollector);
    }
    pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    FailureCollector failureCollector = context.getFailureCollector();
    createRecordTransformConfig.validate(failureCollector);
  }

  @Override
  public void transform(StructuredRecord structuredRecord, Emitter<StructuredRecord> emitter) throws Exception {
    final Schema outputSchema = getContext().getOutputSchema();
    if (outputSchema == null) {
      getContext().getFailureCollector().addFailure("Output schema missing.",
                                                    "Please provide output schema");
    }
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    final JsonElement fieldMappingJson = createRecordTransformConfig.getFieldMappingJson();
    mapFields(builder, structuredRecord, fieldMappingJson, outputSchema);
    emitter.emit(builder.build());
  }

  /**
   * Mpa input data fields to output data fields
   *
   * @param builder          {@link StructuredRecord.Builder} builder set the data for
   * @param oldRecord        {@link StructuredRecord} existing record to read from
   * @param fieldMappingJson {@link JsonElement} path for the field to read from
   */
  private void mapFields(StructuredRecord.Builder builder, StructuredRecord oldRecord, JsonElement fieldMappingJson,
                         Schema outputSchema) {
    for (Map.Entry<String, JsonElement> treeNode : fieldMappingJson.getAsJsonObject().entrySet()) {
      if (treeNode.getValue().isJsonArray()) {
        builder.set(treeNode.getKey(), getField(oldRecord, treeNode.getValue().getAsJsonArray()));
      }
      if (treeNode.getValue().isJsonObject()) {
        final Schema schema = outputSchema.getField(treeNode.getKey()).getSchema();
        StructuredRecord.Builder builder1 = StructuredRecord.builder(schema);
        mapFields(builder1, oldRecord, treeNode.getValue(), schema);
        builder.set(treeNode.getKey(), builder1.build());
      }
    }
  }

  /**
   * Generate field for output schema from input schema and mapping field data
   *
   * @param inputSchema      {@link Schema}
   * @param collector        {@link FailureCollector}
   * @param recordName       name for the field of type record
   * @param fieldMappingJson {@link JsonElement} path for the field in input record
   * @return returns {@link Schema} generated from mapping
   */
  private Schema generateFields(Schema inputSchema, FailureCollector collector, String recordName,
                                JsonElement fieldMappingJson) {
    List<Schema.Field> fieldList = new ArrayList<>();
    for (Map.Entry<String, JsonElement> treeNode : fieldMappingJson.getAsJsonObject().entrySet()) {
      if (treeNode.getValue().isJsonArray()) {
        final Schema.Field field = getField(inputSchema, treeNode.getValue().getAsJsonArray());
        fieldList.add(Schema.Field.of(treeNode.getKey(), field.getSchema()));
      }
      if (treeNode.getValue().isJsonObject()) {
        final Schema result = generateFields(inputSchema, collector, treeNode.getKey(),
                                             treeNode.getValue().getAsJsonObject());
        fieldList.add(Schema.Field.of(treeNode.getKey(), result));
      }
    }
    return Schema.recordOf(recordName, fieldList);
  }

  /**
   * Generate output schema
   */
  private Schema getOutputSchema(Schema inputSchema, FailureCollector collector) {
    Schema output = schemaCache.get(inputSchema);
    if (output != null) {
      return output;
    }
    boolean includeNonMappedFields = createRecordTransformConfig.getIncludeNonMappedFields().equals("on");
    final JsonElement fieldMappingJson = createRecordTransformConfig.getFieldMappingJson();
    if (fieldMappingJson.isJsonNull()) {
      collector.addFailure("Empty mapping field.", "Please provide valid mapping field.");
    }
    StructuredRecord.Builder builder;
    final Schema schema = generateFields(inputSchema, collector, "record",
                                         fieldMappingJson.getAsJsonObject());
    // TODO - add/ignore out non mapped fields
    return schema;
  }

  /**
   * Get field from structured record
   *
   * @param structuredRecord {@link StructuredRecord} record to read the data from
   * @param pathMap          {@link JsonArray} path of the field to read from
   * @return {@link StructuredRecord} property containing the value
   */
  private Object getField(StructuredRecord structuredRecord, JsonArray pathMap) {
    if (pathMap.size() == 1) {
      return structuredRecord.get(pathMap.get(0).getAsString());
    }
    StructuredRecord value = null;
    while (pathMap.iterator().hasNext()) {
      final JsonElement next = pathMap.iterator().next();
      if (value == null) {
        value = structuredRecord.get(next.getAsString());
      }
      value = value.get(next.getAsString());
    }
    return value;
  }

  /**
   * Get field from schema
   *
   * @param inputSchema {@link Schema} input schema
   * @param pathMap     {@link JsonArray} path of the field to read
   * @return return field found in given path
   */
  private Schema.Field getField(Schema inputSchema, JsonArray pathMap) {
    if (pathMap.size() == 1) {
      return inputSchema.getField(pathMap.get(0).getAsString());
    }
    Schema.Field value = null;
    while (pathMap.iterator().hasNext()) {
      final JsonElement next = pathMap.iterator().next();
      if (value == null) {
        value = inputSchema.getField(next.getAsString());
      }
      value = value.getSchema().getField(next.getAsString());
    }
    return value;
  }
}
