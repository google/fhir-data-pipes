
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.openmrs.analytics;

import static org.mockito.Mockito.when;

import java.beans.PropertyVetoException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.openmrs.analytics.model.EventConfiguration;

@RunWith(MockitoJUnitRunner.class)
public class UuidUtilTest extends TestCase {
	
	@Mock
	private JdbcConnectionUtil jdbcConnectionUtil;
	
	private EventConfiguration config;
	
	private Map<String, String> payLoad;
	
	@Before
	public void beforeTestCase() throws Exception {
		
		when(jdbcConnectionUtil.getJdbcDriverClass()).thenReturn("com.mysql.cj.jdbc.Driver");
		when(jdbcConnectionUtil.getJdbcUrl()).thenReturn("jdbc:mysql://localhost:3308/openmrs");
		when(jdbcConnectionUtil.getDbUser()).thenReturn("root");
		when(jdbcConnectionUtil.getDbPassword()).thenReturn("debezium");
		when(jdbcConnectionUtil.getInitialPoolSize()).thenReturn(10);
		when(jdbcConnectionUtil.getDbcMaxPoolSize()).thenReturn(50);
		
		payLoad = new HashMap<>();
		config = new EventConfiguration();
		
		payLoad.put("uuid", "1");
		payLoad.put("sex", "male");
		
		config.setParentTable("person");
		config.setParentForeignKey("person_id");
		config.setChildPrimaryKey("patient_id");
	}
	
	@Test
	public void shouldCallGetUuid() throws ClassNotFoundException, PropertyVetoException, SQLException {
		
		UuidUtil uuidUtil = new UuidUtil(jdbcConnectionUtil);
		
		String uuid = uuidUtil.getUuid(config.getParentTable(), config.getParentForeignKey(),
		    payLoad.get(config.getChildPrimaryKey()).toString());
		
		assertNotNull(uuid);
		
		assertEquals(uuid, "1296b0dc-440a-11e6-a65c-00e04c680037");
		
	}
}
