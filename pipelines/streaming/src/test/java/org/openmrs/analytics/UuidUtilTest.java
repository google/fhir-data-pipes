/*
 * Copyright 2020-2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.openmrs.analytics;

import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class UuidUtilTest extends TestCase {

  @Mock private JdbcConnectionUtil jdbcConnectionUtil;

  @Mock private Statement statement;

  @Mock private ResultSet resultset;

  private String uuid;

  private String table;

  private String keyColumn;

  private String keyValue;

  @Before
  public void beforeTestCase() throws Exception {
    uuid = "1296b0dc-440a-11e6-a65c-00e04c680037";
    table = "person";
    keyColumn = "person_id";
    keyValue = "5";

    String sql = String.format("SELECT uuid FROM %s WHERE %s = %s", table, keyColumn, keyValue);

    when(jdbcConnectionUtil.createStatement()).thenReturn(statement);
    when(statement.executeQuery(sql)).thenReturn(resultset);
    when(resultset.next()).thenReturn(true).thenReturn(false);
    when(resultset.getString("uuid")).thenReturn(uuid);
  }

  @Test
  public void shouldReturnValidUuid() throws SQLException {

    UuidUtil uuidUtil = new UuidUtil(jdbcConnectionUtil);
    String uuid = uuidUtil.getUuid(table, keyColumn, keyValue);

    assertNotNull(uuid);
    assertEquals(uuid, "1296b0dc-440a-11e6-a65c-00e04c680037");
  }
}
