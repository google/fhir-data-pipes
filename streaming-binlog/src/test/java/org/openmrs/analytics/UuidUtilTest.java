
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

import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.beans.PropertyVetoException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.OngoingStubbing;
import org.openmrs.analytics.model.EventConfiguration;

@RunWith(MockitoJUnitRunner.class)
public class UuidUtilTest extends TestCase {

	@Mock
	private JdbcConnectionUtil jdbcConnectionUtil;

	@Mock
	private Statement statement;

	@Mock
	ResultSet rs;

	@Mock
	EventConfiguration config;

	//@Mock
	private Map<String, String>payLoad;
 
	// @Mock
	// private ComboPooledDataSource comboPooledDataSource;
	
	// @Mock
	//private UuidUtil uuidUtil;

	private String person = "person";

	private String personid = "person_id";

	private String patientid = "patient_id";


	@Before
    public void beforeTestCase() throws Exception {

		when(jdbcConnectionUtil.getJdbcDriverClass()).thenReturn("com.mysql.cj.jdbc.Driver");
		when(jdbcConnectionUtil.getJdbcUrl()).thenReturn("jdbc:mysql://localhost:3308/openmrs");
		when(jdbcConnectionUtil.getDbUser()).thenReturn("root");
		when(jdbcConnectionUtil.getDbPassword()).thenReturn("debezium");
		when(jdbcConnectionUtil.getInitialPoolSize()).thenReturn(10);
		when(jdbcConnectionUtil.getDbcMaxPoolSize()).thenReturn(50);

		// when(comboPooledDataSource.getDriverClass()).thenReturn("com.mysql.cj.jdbc.Driver");
		// when(comboPooledDataSource.getJdbcUrl()).thenReturn("jdbc:mysql://localhost:3308/openmrs");
		// when(comboPooledDataSource.getUser()).thenReturn("root");
		// when(comboPooledDataSource.getPassword()).thenReturn("debezium");
		// when(comboPooledDataSource.getInitialPoolSize()).thenReturn(10);
		// when(comboPooledDataSource.getMaxPoolSize()).thenReturn(50);


		when(jdbcConnectionUtil.createStatement()).thenReturn(statement);
		//when(jdbcConnectionUtil.getConnectionObject()).thenReturn(comboPooledDataSource);
		payLoad = new HashMap<>();
		config = new EventConfiguration();

		// Mockito.when(config.getParentTable()).thenReturn(person);
		// Mockito.when(config.getParentForeignKey()).thenReturn(patientid);
		// Mockito.when(config.getChildPrimaryKey()).thenReturn(personid);
	
		payLoad.put("uuid", "1");
		payLoad.put("sex", "male");


		config.setParentTable("person");
		config.setParentForeignKey("person_id");
		config.setChildPrimaryKey("patient_id");


		// when(config.getParentTable()).thenReturn("person");
		// when(config.getParentForeignKey()).thenReturn("person_id");
		// when(config.getChildPrimaryKey()).thenReturn("patient_id");

    }

	@Test
	public void shouldCallGetUuid() throws ClassNotFoundException, PropertyVetoException, SQLException {

		//jdbcConnectionUtil.
		UuidUtil uuidUtil = new UuidUtil(jdbcConnectionUtil);

		String uuid = uuidUtil.getUuid(config.getParentTable(), config.getParentForeignKey(), payLoad.get(config.getChildPrimaryKey()).toString());

		assertNotNull(uuid);

		assertEquals(uuid, "1296b0dc-440a-11e6-a65c-00e04c680037");

		// //when(jdbcConnectionUtil.getConnectionObject().thenReturn(comboPooledDataSource));

		// when(((OngoingStubbing) jdbcConnectionUtil.createStatement()).thenReturn(statement));
		
		// //lenient().when(uuidUtil.getUuid(config.getParentTable(), config.getParentForeignKey(), config.getChildPrimaryKey()).thenReturn(uuid));
		// assertEquals(uuid, "1296b0dc-440a-11e6-a65c-00e04c680037");
		
		// Mockito.verify(uuidUtil, times(1)).getUuid(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
		// Mockito.verify(uuidUtil).getUuid(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
		
	}
}
