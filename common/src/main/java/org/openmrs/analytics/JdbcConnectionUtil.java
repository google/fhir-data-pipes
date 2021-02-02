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

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcConnectionUtil {
	
	private static final Logger log = LoggerFactory.getLogger(JdbcConnectionUtil.class);
	
	private static ComboPooledDataSource comboPooledDataSource;
	
	private String jdbcDriverClass;
	
	private String jdbcUrl;
	
	private String dbUser;
	
	private String dbPassword;
	
	private Integer initialPoolSize;
	
	private Integer dbcMaxPoolSize;
	
	JdbcConnectionUtil(String jdbcDriverClass, String jdbcUrl, String dbUser, String dbPassword, int dbcMaxPoolSize,
	    int initialPoolSize) {
		
		this.jdbcDriverClass = jdbcDriverClass;
		this.jdbcUrl = jdbcUrl;
		this.dbUser = dbUser;
		this.dbPassword = dbPassword;
		this.dbcMaxPoolSize = dbcMaxPoolSize;
		this.initialPoolSize = initialPoolSize;
	}
	
	public Statement createStatement() throws SQLException, PropertyVetoException {
		Connection con = getConnectionObject().getConnection();
		return con.createStatement();
	}
	
	public ComboPooledDataSource getConnectionObject() throws PropertyVetoException {
		comboPooledDataSource = new ComboPooledDataSource();
		comboPooledDataSource.setDriverClass(this.jdbcDriverClass);
		comboPooledDataSource.setJdbcUrl(this.jdbcUrl);
		comboPooledDataSource.setUser(this.dbUser);
		comboPooledDataSource.setPassword(this.dbPassword);
		comboPooledDataSource.setMaxPoolSize(this.dbcMaxPoolSize);
		comboPooledDataSource.setInitialPoolSize(10);
		return comboPooledDataSource;
	}
	
}
