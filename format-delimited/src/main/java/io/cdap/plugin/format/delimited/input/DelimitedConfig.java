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
import com.google.common.io.Files;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.validation.FormatContext;
import io.cdap.plugin.format.input.PathTrackingConfig;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
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

  public Schema getSchema(FormatContext context) {
    if (containsMacro(NAME_SCHEMA)) {
      return null;
    }
    if (schema == null) {
      try {
        return getDefaultSchema(context);
      } catch (IOException e) {
        throw new IllegalArgumentException("Invalid schema: " + e.getMessage(), e);
      }
    }
    try {
      return Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid schema: " + e.getMessage(), e);
    }
  }

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
  public Schema getDefaultSchema(FormatContext context) throws IOException {
    final String format = getProperties().getProperties().getOrDefault(FORMAT, "delimited");
    String delimiter = getProperties().getProperties().get(DELIMITER);
    if (format.equals("delimited") && Strings.isNullOrEmpty(delimiter)) {
      throw new IllegalArgumentException("Delimiter is required when format is set to 'delimited'");
    }
    List<Schema.Field> fields = new ArrayList<>();
    String path = getProperties().getProperties().getOrDefault(
        "path", ""
    );

    File file = pickFile(path, format.equals("delimited") ? null : format);
    BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
    String line = bufferedReader.readLine();
    if (line == null) {
      return null;
    }
    bufferedReader.close();
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

  /**
   * Checks whether provided path is directory or file and returns file based on the following
   * conditions: if provided path directs to file - file from the provided path will be returned if
   * provided path directs to a directory - first file matching the extension will be provided if
   * extension is null first file from the directory will be returned
   *
   * @param path              path from config
   * @param matchingExtension extension to match when searching for file in directory
   * @return {@link File}
   */
  private File pickFile(String path, String matchingExtension) {
    final File filePath = Paths.get(path).toFile();
    if (filePath.isFile()) {
      return filePath;
    }
    // read directory files
    final File[] files = filePath.listFiles();
    if (files == null) {
      throw new IllegalArgumentException("Cannot read files from provided path");
    }
    if (files.length == 0) {
      throw new IllegalArgumentException("Provided directory is empty");
    }
    // find first delimited file
    for (File file : files) {
      if (matchingExtension == null) {
        return file;
      }
      if (Files.getFileExtension(file.getName()).equals(matchingExtension)) {
        return file;
      }
    }
    throw new IllegalArgumentException("Could not find a delimited file found in provided path");
  }
}
