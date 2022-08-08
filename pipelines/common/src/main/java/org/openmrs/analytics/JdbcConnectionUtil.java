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

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.google.common.base.Preconditions;
import com.mchange.v2.c3p0.ComboPooledDataSource;

public class JdbcConnectionUtil {
	
	private final ComboPooledDataSource comboPooledDataSource;
	
	JdbcConnectionUtil(String jdbcDriverClass, String jdbcUrl, String dbUser, String dbPassword, int initialPoolSize,
	    int dbcMaxPoolSize) throws PropertyVetoException {
		Preconditions.checkArgument(initialPoolSize <= dbcMaxPoolSize,
		    "initialPoolSize cannot be larger than dbcMaxPoolSize");
		comboPooledDataSource = new ComboPooledDataSource();
		comboPooledDataSource.setDriverClass(jdbcDriverClass);
		comboPooledDataSource.setJdbcUrl(jdbcUrl);
		comboPooledDataSource.setUser(dbUser);
		comboPooledDataSource.setPassword(dbPassword);
		comboPooledDataSource.setMaxPoolSize(dbcMaxPoolSize);
		comboPooledDataSource.setInitialPoolSize(initialPoolSize);
	}
	
	public Statement createStatement() throws SQLException {
		Connection con = getConnectionObject().getConnection();
		return con.createStatement();
	}
	
	public void closeConnection(Statement stmt) throws SQLException {
		if (stmt != null) {
			Connection con = stmt.getConnection();
			stmt.close();
			con.close();
		}
	}
	
	public ComboPooledDataSource getConnectionObject() {
		return comboPooledDataSource;
	}
}
