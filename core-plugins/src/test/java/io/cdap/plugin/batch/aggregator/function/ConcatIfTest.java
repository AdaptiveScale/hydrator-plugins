/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.batch.aggregator.function;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.Schema.Type;
import org.junit.Test;

import java.util.Arrays;

/**
 * A test method for Concat aggregate function
 */
public class ConcatIfTest extends AggregateFunctionTest {

  @Test
  public void testCondition() {
    String condition = "x != null && !x.equals(\"2\") && !x.equals(\"3\")";
    Schema fieldSchema = Schema.of(Type.STRING);
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.nullableOf(fieldSchema)));
    test(new ConcatIf("x", fieldSchema, JexlCondition.of(condition)), schema, "x", "1, 4, 5",
         Arrays.asList("1", "2", "3", "4", "5"), new ConcatIf("x", fieldSchema, JexlCondition.of(condition)));
    test(new ConcatIf("x", fieldSchema, JexlCondition.of(condition)), schema, "x", "4, 5",
         Arrays.asList(null, "2", "4", "3", "5"), new ConcatIf("x", fieldSchema, JexlCondition.of(condition)));
  }

}
