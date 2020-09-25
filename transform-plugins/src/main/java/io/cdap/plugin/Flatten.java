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

package io.cdap.plugin;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Flatten is a transform plugin that flattens nested data structures.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("Flatten")
@Description("Flatten is a transform plugin that flattens nested data structures.")
public final class Flatten extends Transform<StructuredRecord, StructuredRecord> {

  private Config config;
  private Schema outputSchema;
  private Map<String, OutputFieldInfo> inputOutputMapping = Maps.newHashMap();

  public Flatten(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();

    if (inputSchema == null) {
      return;
    }

    List<String> fieldsToFlatten = config.getFieldsToFlatten();
    Integer levelToLimitFlattening = config.getLevelToLimitFlattening();
    // properties can be macro
    if (fieldsToFlatten == null || levelToLimitFlattening == null
      || config.containsMacro(Config.PROPERTY_NAME_PREFIX)) {
      return;
    }

    checkSchemaType(inputSchema, fieldsToFlatten, collector);
    collector.getOrThrowException();

    List<OutputFieldInfo> inputOutputMapping = createOutputFieldsInfo(inputSchema, fieldsToFlatten,
                                                                      config.getLevelToLimitFlattening());
    Map<String, OutputFieldInfo> outputFieldMap = handleDuplicateFieldName(inputOutputMapping, config.prefix);
    Schema outputSchema = generateOutputSchema(inputSchema, outputFieldMap.values());
    pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
  }

  private void checkSchemaType(Schema inputSchema, List<String> fieldsToFlatten, FailureCollector collector) {
    for (String field : fieldsToFlatten) {
      Schema schemaField = inputSchema.getField(field).getSchema();
      if (!isRecord(schemaField)) {
        collector.addFailure(String.format("'%s' cannot be flattened.", field),
                             "Only fields with schema type of `record` can be flattened.");
      }
    }
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);

    FailureCollector failureCollector = context.getFailureCollector();

    List<String> fieldsToFlatten = config.getFieldsToFlatten();
    if (fieldsToFlatten == null || fieldsToFlatten.isEmpty()) {
      failureCollector.addFailure("Atleast one field should be selected.", "");
      failureCollector.getOrThrowException();
    }

    Schema inputSchema = context.getInputSchema();
    if (inputSchema == null) {
      return;
    }

    List<OutputFieldInfo> inputOutputMapping = createOutputFieldsInfo(inputSchema, fieldsToFlatten,
                                                                      config.getLevelToLimitFlattening());
    this.inputOutputMapping = handleDuplicateFieldName(inputOutputMapping, config.getPrefix());
    this.outputSchema = generateOutputSchema(inputSchema, inputOutputMapping);
  }


  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    if (outputSchema == null) {
      List<OutputFieldInfo> inputOutputMapping = createOutputFieldsInfo(input.getSchema(), config.getFieldsToFlatten(),
                                                                        config.getLevelToLimitFlattening());
      this.inputOutputMapping = handleDuplicateFieldName(inputOutputMapping, config.getPrefix());
      this.outputSchema = generateOutputSchema(input.getSchema(), inputOutputMapping);
    }
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);

    for (Schema.Field field : outputSchema.getFields()) {
      OutputFieldInfo outputFieldInfo = inputOutputMapping.get(field.getName());
      Object value = outputFieldInfo.getValue(input);
      builder.set(field.getName(), value);
    }
    emitter.emit(builder.build());
  }

  private List<OutputFieldInfo> createOutputFieldsInfo(Schema inputSchema, List<String> fieldsToFlatten,
                                                       int levelToLimitFlattening) {
    List<Schema.Field> fields = inputSchema.getFields();
    List<OutputFieldInfo> mapping = new ArrayList<>();

    if (fields == null || fields.isEmpty()) {
      return new ArrayList<>();
    }
    for (Schema.Field field : fields) {
      String name = field.getName();
      if (fieldsToFlatten.contains(name)) {
        mapping.addAll(flattenField(field, levelToLimitFlattening));
      } else {
        mapping.add(generateOutputFieldInfo(field));
      }
    }
    return mapping;
  }

  private List<OutputFieldInfo> flattenField(Schema.Field field, int levelToLimitFlattening) {
    Schema schema = field.getSchema();

    if (isRecord(schema) && levelToLimitFlattening != 0) {
      return generateOutputFieldInfoForRecord(field, levelToLimitFlattening);
    }

    ArrayList<OutputFieldInfo> maps = new ArrayList<>();
    maps.add(generateOutputFieldInfo(field));
    return maps;
  }

  private OutputFieldInfo generateOutputFieldInfo(Schema.Field field) {
    return generateOutputFieldInfo(field, field.getSchema());
  }

  private OutputFieldInfo generateOutputFieldInfo(Schema.Field field, Schema fieldSchema) {
    Node node = new Node();
    node.fieldName = field.getName();
    node.fieldSchema = fieldSchema;

    OutputFieldInfo outputFieldInfo = new OutputFieldInfo();
    outputFieldInfo.fieldName = field.getName();
    outputFieldInfo.node = node;
    return outputFieldInfo;
  }

  /**
   * Try to flatten fields of Records. <br>
   * For each field of record check if type is:  <br>
   *
   * <p><b>Record</b>: try to flatten sub fields recursively until schema of field is not record or `levelToLimitFlattening`
   * is reached and return {@link OutputFieldInfo} for flatten fields</p>
   * <p></p><b>is not record</b>: return {@link OutputFieldInfo} for not flatten records</p>

   * <pre>
   *  Example:
   *    {
   *      "homeTeam" : {
   *          "name" : "TeamA",
   *          "players": ["Player1", "Player2", "Player3"],
   *          "address": {
   *              "name":"CityA",
   *              "code": 1200
   *          }
   *          "code" : 500
   *      },
   *      "awayTeam" : {
   *         "name" : "TeamA",
   *         "players": ["Player1", "Player2", "Player3"],
   *         "address": {
   *              "name":"CityB",
   *              "code": 1201
   *          }
   *         "code" : 501
   *      }
   *    }</pre>
   * <p>
   * Result will be list of OutputFieldInfo:
   * <p>
   * fieldName: homeTeam_name and node: homeTeam -> name
   * fieldName: homeTeam_players and node: homeTeam -> players
   * fieldName: homeTeam_address_name and node: homeTeam -> address-> name
   * fieldName: homeTeam_address_code and node: homeTeam -> address-> code
   * fieldName: homeTeam_code and node: home -> code
   * <p>
   * fieldName: awayTeam_name and node: awayTeam -> name
   * fieldName: awayTeam_players and node: awayTeam -> players
   * fieldName: awayTeam_address_name and node: awayTeam -> address-> name
   * fieldName: awayTeam_address_code and node: awayTeam -> address-> code
   * fieldName: awayTeam_code and node: awayTeam -> code
   * <p>
   * and the OutputSchema Fields will be
   * <pre>
   *       homeTeam_name,
   *       homeTeam_players,
   *       homeTeam_address_name,
   *       homeTeam_address_code,
   *       homeTeam_code,
   *       awayTeam_name,
   *       awayTeam_players,
   *       awayTeam_address_name,
   *       awayTeam_address_code,
   *       awayTeam_code
   * </pre>
   *
   * @param field                  Field to flatten
   * @param levelToLimitFlattening
   * @return list of  {@link OutputFieldInfo}
   */
  private List<OutputFieldInfo> generateOutputFieldInfoForRecord(Schema.Field field, int levelToLimitFlattening) {

    List<OutputFieldInfo> outputFieldInfos = new ArrayList<>();
    Schema schema = field.getSchema();
    List<Schema.Field> fields;

    fields = schema.isNullable() ? schema.getNonNullable().getFields() : schema.getFields();

    if (fields == null || fields.size() == 0) {
      return outputFieldInfos;
    }

    for (Schema.Field subField : fields) {
      outputFieldInfos.addAll(flattenField(subField, levelToLimitFlattening - 1));
    }

    List<OutputFieldInfo> result = new ArrayList<>();
    for (OutputFieldInfo outputFieldInfo : outputFieldInfos) {
      result.add(OutputFieldInfo.fromChild(outputFieldInfo, field));
    }
    return result;
  }

  private Map<String, OutputFieldInfo> handleDuplicateFieldName(List<OutputFieldInfo> inputOutputMapping,
                                                                String prefix) {
    Map<String, OutputFieldInfo> result = new HashMap<>();
    for (OutputFieldInfo outputFieldInfo : inputOutputMapping) {
      if (result.containsKey(outputFieldInfo.fieldName)) {
        outputFieldInfo.fieldName = String.format("%s_%s", prefix, outputFieldInfo.fieldName);
      }
      result.put(outputFieldInfo.fieldName, outputFieldInfo);
    }
    return result;
  }

  private Schema generateOutputSchema(Schema inputSchema, @Nonnull Collection<OutputFieldInfo> outputFieldInfos) {
    List<Schema.Field> fields = outputFieldInfos
      .stream()
      .map(outputFieldInfo -> Schema.Field.of(outputFieldInfo.fieldName, outputFieldInfo.getSchema()))
      .collect(Collectors.toList());
    return Schema.recordOf(inputSchema.getRecordName() + ".flatten", fields);
  }

  private boolean isRecord(Schema schema) {
    if (schema == null) {
      return false;
    }
    Schema.Type type = schema.isNullable() ? schema.getNonNullable().getType() : schema.getType();
    return type == Schema.Type.RECORD;
  }

  /**
   * JSONParser Plugin Config.
   */
  public static class Config extends PluginConfig {

    public static final String PROPERTY_NAME_FIELDS_TO_MAP = "fieldsToFlatten";
    public static final String PROPERTY_NAME_LEVEL_TO_LIMIT = "levelToLimitFlattening";
    public static final String PROPERTY_NAME_PREFIX = "prefix";

    @Macro
    @Name(PROPERTY_NAME_FIELDS_TO_MAP)
    @Description("Specifies the list of fields in the input schema to be flattened.")
    private String fieldsToFlatten;

    @Macro
    @Nullable
    @Name(PROPERTY_NAME_LEVEL_TO_LIMIT)
    @Description("Limit flattening to a certain level in nested structures. Default is 1.")
    private String levelToLimitFlattening;

    @Macro
    @Nullable
    @Name(PROPERTY_NAME_PREFIX)
    @Description("An optional prefix to be used while generating names of the flattened fields. " +
      "This can be used to fix conflicts that can occur if fields have the same name after flattening. " +
      "The prefix is added as <prefix>_<flattened_name >")
    private String prefix;

    public Config(String fieldsToFlatten, @Nullable String levelToLimitFlattening, @Nullable String prefix) {
      this.fieldsToFlatten = fieldsToFlatten;
      this.levelToLimitFlattening = levelToLimitFlattening;
      this.prefix = prefix;
    }

    public List<String> getFieldsToFlatten() {
      if (containsMacro(PROPERTY_NAME_FIELDS_TO_MAP) || Strings.isNullOrEmpty(fieldsToFlatten)) {
        return null;
      }
      return Lists.newArrayList(Splitter.on(",").trimResults().split(fieldsToFlatten));
    }

    public Integer getLevelToLimitFlattening() {
      if (containsMacro(PROPERTY_NAME_LEVEL_TO_LIMIT)) {
        return null;
      }
      return Strings.isNullOrEmpty(levelToLimitFlattening) ? 1 : Integer.parseInt(levelToLimitFlattening);
    }

    public String getPrefix() {
      if (containsMacro(PROPERTY_NAME_PREFIX)) {
        return null;
      }
      return prefix;
    }
  }

  /**
   * Class representing schema output {@link OutputFieldInfo#node}. Where node property represent path to reach value
   * in StructuredRecord, ex. field -> subField -> sub_subField
   */
  private static final class OutputFieldInfo {

    /**
     * Output field name
     */
    private String fieldName;

    /**
     * Starting node of the path for value in StructuredRecord
     */
    private Node node;

    /**
     * @param object StructuredRecord
     * @return value
     */
    public Object getValue(StructuredRecord object) {
      return node.getValue(object);
    }

    /**
     * @return Output Field Schema
     */
    public Schema getSchema() {
      return node.getOutputSchema();
    }

    public static OutputFieldInfo fromChild(OutputFieldInfo outputFieldInfo, Schema.Field field) {
      OutputFieldInfo fieldMap = new OutputFieldInfo();
      fieldMap.fieldName = String.format("%s_%s", field.getName(), outputFieldInfo.fieldName);
      fieldMap.node = Node.nodeWithChild(outputFieldInfo.node, field);
      return fieldMap;
    }
  }

  /**
   * Class representing field name and field schema in StructuredRecord
   */
  private static final class Node {

    /**
     * Field name in Structured Record
     */
    private String fieldName;

    /**
     * Next node in the path
     */
    private Node next;

    /**
     * Field Schema for fieldName
     */
    private Schema fieldSchema;

    /**
     * Get value in StructuredRecord, if {@link Node#next} property is not null, value for Output Field is inside
     * StructuredRecord
     *
     * @param object StructuredRecord
     * @return value of the field in StructuredRecord
     */
    public Object getValue(StructuredRecord object) {
      Object value = object.get(fieldName);
      if (next == null) {
        return value;
      }
      if (value == null) {
        return null;
      }
      return next.getValue((StructuredRecord) value);
    }

    /**
     * Get Output Field Schema
     *
     * @return Schema
     */
    public Schema getOutputSchema() {
      if (next == null) {
        return fieldSchema;
      }
      return next.getOutputSchema();
    }

    public static Node nodeWithChild(Node childNode, Schema.Field field) {
      Node node = new Node();
      node.fieldName = field.getName();
      node.next = childNode;
      node.fieldSchema = field.getSchema();
      return node;
    }
  }
}
