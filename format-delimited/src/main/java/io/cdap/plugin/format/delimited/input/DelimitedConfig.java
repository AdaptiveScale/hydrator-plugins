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

package io.cdap.plugin.format.delimited.input;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.validation.FormatContext;
import io.cdap.plugin.common.batch.JobUtils;
import io.cdap.plugin.format.input.PathTrackingConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Common config for delimited related formats
 */
public class DelimitedConfig extends PathTrackingConfig {
  public static final Map<String, PluginPropertyField> DELIMITED_FIELDS;
  private static final String SKIP_HEADER_DESC = "Whether to skip the first line of each file. " +
    "Default value is false.";
  private static final String DELIMITER = "delimiter";
  private static final String FORMAT = "format";

  static {
    Map<String, PluginPropertyField> fields = new HashMap<>(FIELDS);
    fields.put("skipHeader", new PluginPropertyField("skipHeader", SKIP_HEADER_DESC,
                                                     "boolean", false, true));
    DELIMITED_FIELDS = Collections.unmodifiableMap(fields);
  }

  @Macro
  @Nullable
  @Description(SKIP_HEADER_DESC)
  protected Boolean skipHeader;

  public boolean getSkipHeader() {
    return skipHeader == null ? false : skipHeader;
  }

  @Nullable
  @Override
  public Schema getSchema() {
    if (containsMacro(NAME_SCHEMA)) {
      return null;
    }
    if (schema == null || schema.equals("")) {
      try {
        return getDefaultSchema(null);
      } catch (IOException e) {
        throw new IllegalArgumentException("Invalid schema: " + e.getMessage(), e);
      }
    }
    return super.getSchema();
  }

  /**
   * Reads delimiter from config
   * If not available returns default delimiter based on format
   * @return delimiter
   */
  private String getDefaultDelimiter() {
    String delimiter = getProperties().getProperties().get(DELIMITER);
    if (delimiter != null) {
      return delimiter;
    }
    final String format = getProperties().getProperties().get(FORMAT);
    switch (format) {
      case "tsv":
        return "\t";
      default:
        return ",";
    }
  }

  /**
   * Extract schema from file
   *
   * @param context {@link FormatContext}
   * @return {@link Schema}
   * @throws IOException raised when error occurs during schema extraction
   */
  public Schema getDefaultSchema(@Nullable FormatContext context) throws IOException {
    final String format = getProperties().getProperties().getOrDefault(FORMAT, "delimited");
    String delimiter = getProperties().getProperties().get(DELIMITER);
    if (format.equals("delimited") && Strings.isNullOrEmpty(delimiter)) {
      throw new IllegalArgumentException("Delimiter is required when format is set to 'delimited'.");
    }
    List<Schema.Field> fields = new ArrayList<>();
    String path = getProperties().getProperties().getOrDefault(
      "path", ""
    );

    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();
    // set entries here, before FileSystem is used
    for (Map.Entry<String, String> entry : getFileSystemProperties().entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    FSDataInputStream input = null;
    BufferedReader bufferedReader = null;
    String line = null;
    try {
      final Path file = getFilePathForSchemaGeneration(path,
                                                       format.equals("delimited") ? null : format, conf);
      final FileSystem fileSystem = FileSystem.get(file.toUri(), conf);
      input = fileSystem.open(file);
      bufferedReader = new BufferedReader(new InputStreamReader(input));
      line = bufferedReader.readLine();
      if (line == null) {
        return null;
      }
    } finally {
      if (bufferedReader != null) {
        bufferedReader.close();
      }
      if (input != null) {
        input.close();
      }
    }
    String[] columns = line.split(getDefaultDelimiter());
    int count = 1;
    for (String column : columns) {
      if (getSkipHeader()) {
        fields.add(Schema.Field.of(column, Schema.of(Schema.Type.STRING)));
        continue;
      }
      fields.add(
        Schema.Field.of(
          String.format("%s_%s", "body", count),
          Schema.of(Schema.Type.STRING)
        )
      );
      count++;
    }
    return Schema.recordOf("text", fields);
  }

}
