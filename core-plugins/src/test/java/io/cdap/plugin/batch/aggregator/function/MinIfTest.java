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
import org.junit.Test;

public class MinIfTest extends NumberTest {

  @Test
  public void testIntCondition() {
    String condition = "x.equals(99)";

    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    Min min = new MinIf("x", Schema.of(Schema.Type.INT), JexlCondition.of(condition));
    Min min1 = new MinIf("x", Schema.of(Schema.Type.INT), JexlCondition.of(condition));
    testFunction(min, schema, min1, 99, 99, 100, 101);
  }

  @Test
  public void testLongCondition() {
    String condition = "x<1L";

    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.LONG)));
    Min min = new MinIf("x", Schema.of(Schema.Type.LONG), JexlCondition.of(condition));
    Min min1 = new MinIf("x", Schema.of(Schema.Type.LONG), JexlCondition.of(condition));
    testFunction(min, schema, min1, Long.MIN_VALUE, -1L, 0L, Long.MIN_VALUE, 500L);
    testFunction(min, schema, min1, 0L, 0L);
  }

  @Test
  public void testFloatCondition() {
    String condition = "x<1f";

    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.FLOAT)));
    Min min = new MinIf("x", Schema.of(Schema.Type.FLOAT), JexlCondition.of(condition));
    Min min1 = new MinIf("x", Schema.of(Schema.Type.FLOAT), JexlCondition.of(condition));
    testFunction(min, schema, min1, -1.1f, -1.1f, 0f, Float.MIN_NORMAL, 500.2f);
  }

  @Test
  public void testDoubleCondition() {
    String condition = "x<1d";

    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.DOUBLE)));
    Min min = new MinIf("x", Schema.of(Schema.Type.DOUBLE), JexlCondition.of(condition));
    Min min1 = new MinIf("x", Schema.of(Schema.Type.DOUBLE), JexlCondition.of(condition));
    testFunction(min, schema, min1, -1.1d, -1.1d, 0d, Double.MIN_NORMAL, 500.2d);
  }
}
