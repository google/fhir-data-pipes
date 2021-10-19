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

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class JdbcConnectionUtilTest {
	
	private Connection mockConnection;
	
	private ResultSet mockResultSet;
	
	private Statement mockStatement;
	
	private JdbcConnectionUtil jdbcConnectionUtil;
	
	@Before
	public void setup() throws PropertyVetoException, SQLException {
		mockResultSet = mock(ResultSet.class);
		mockStatement = mock(Statement.class);
		mockConnection = mock(Connection.class);
		when(mockStatement.getConnection()).thenReturn(mockConnection);
		jdbcConnectionUtil = new JdbcConnectionUtil("random", "random", "omar", "123", 3, 60);
		
	}
	
	@Test
	public void testSetIncorrectJdbcPoolSize() throws PropertyVetoException {
		PropertyVetoException thrown = Assert.assertThrows(PropertyVetoException.class,
		    () -> new JdbcConnectionUtil("random", "random", "omar", "123", 4, 2));
		
		assertTrue(thrown.getMessage().contains("initialPoolSize cannot be larger than dbcMaxPoolSize"));
	}
	
	@Test
	public void testCloseConnection() throws PropertyVetoException, SQLException {
		
		jdbcConnectionUtil.closeConnection(mockResultSet, mockStatement);
		verify(mockResultSet, times(1)).close();
		verify(mockStatement, times(1)).close();
		verify(mockConnection, times(1)).close();
	}
	
	@Test
	public void testCloseConnectionNullResult() throws PropertyVetoException, SQLException {
		jdbcConnectionUtil.closeConnection(null, mockStatement);
		verify(mockResultSet, times(0)).close();
		verify(mockStatement, times(1)).close();
		verify(mockConnection, times(1)).close();
	}
	
	@Test
	public void testCloseConnectionNullStatement() throws PropertyVetoException, SQLException {
		jdbcConnectionUtil.closeConnection(mockResultSet, null);
		verify(mockResultSet, times(1)).close();
		verify(mockStatement, times(0)).close();
		verify(mockConnection, times(0)).close();
	}
	
	@Test
	public void testCloseConnectionNullBoth() throws PropertyVetoException, SQLException {
		jdbcConnectionUtil.closeConnection(null, null);
		verify(mockResultSet, times(0)).close();
		verify(mockStatement, times(0)).close();
		verify(mockConnection, times(0)).close();
	}
}
