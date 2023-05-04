/*
 * Copyright 2020-2023 Google LLC
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

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.beans.PropertyVetoException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.sql.DataSource;
import org.openmrs.analytics.model.DatabaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcConnectionPools {
  private static final Logger log = LoggerFactory.getLogger(JdbcConnectionPools.class);

  private static final ConcurrentMap<DataSourceConfig, DataSource> dataSources =
      new ConcurrentHashMap<>();

  // This class should not be instantiated!
  private JdbcConnectionPools() {}

  /**
   * Creates a new connection pool for the given config or return an already created one if one
   * exists. The pool size parameters are just hints and won't be used if a pool already exists for
   * the given configuration. This method is to impose a Singleton pattern for each config.
   *
   * @param config the JDBC connection information
   * @param initialPoolSize initial pool size if a new pool is created.
   * @param jdbcMaxPoolSize maximum pool size if a new pool is created.
   * @return
   */
  public static synchronized DataSource getPooledDataSource(
      DataSourceConfig config, int initialPoolSize, int jdbcMaxPoolSize)
      throws PropertyVetoException {
    if (!dataSources.containsKey(config)) {
      dataSources.put(config, createNewPool(config, initialPoolSize, jdbcMaxPoolSize));
    }
    return dataSources.get(config);
  }

  private static DataSource createNewPool(
      DataSourceConfig config, int initialPoolSize, int jdbcMaxPoolSize)
      throws PropertyVetoException {
    log.info(
        "Creating a JDBC connection pool for "
            + config.jdbcUrl()
            + " with driver class "
            + config.jdbcDriverClass()
            + " and max pool size "
            + jdbcMaxPoolSize);
    Preconditions.checkArgument(
        initialPoolSize <= jdbcMaxPoolSize,
        "initialPoolSize cannot be larger than jdbcMaxPoolSize");
    // Note caching of these connection-pools is important beyond just performance benefits. If a
    // `ComboPooledDataSource` goes out of scope without calling `close()` on it, then it can leak
    // connections (and memory) as its threads are not killed and can hold those objects; this was
    // the case even with `setNumHelperThreads(0)`.
    ComboPooledDataSource comboPooledDataSource = new ComboPooledDataSource();
    comboPooledDataSource.setDriverClass(config.jdbcDriverClass());
    comboPooledDataSource.setJdbcUrl(config.jdbcUrl());
    comboPooledDataSource.setUser(config.dbUser());
    comboPooledDataSource.setPassword(config.dbPassword());
    comboPooledDataSource.setMaxPoolSize(jdbcMaxPoolSize);
    comboPooledDataSource.setInitialPoolSize(initialPoolSize);
    // Setting an idle time to reduce the number of connections when idle.
    comboPooledDataSource.setMaxIdleTime(30);
    // Lowering the minimum pool size to limit the number of connections if multiple pools are
    // created for the same DB.
    comboPooledDataSource.setMinPoolSize(1);
    return comboPooledDataSource;
  }

  // TODO we should `close()` connection pools on application shutdown.

  public static DataSourceConfig dbConfigToDataSourceConfig(DatabaseConfiguration config) {
    return DataSourceConfig.create(
        config.getJdbcDriverClass(),
        config.makeJdbsUrlFromConfig(),
        config.getDatabaseUser(),
        config.getDatabasePassword());
  }

  /**
   * This is to identify a database connection pool. We use instances of these to cache connection
   * pools and to impose a Singleton pattern per connection config (hence AutoValue).
   */
  @AutoValue
  public abstract static class DataSourceConfig {
    abstract String jdbcDriverClass();

    abstract String jdbcUrl();

    abstract String dbUser();

    abstract String dbPassword();

    static DataSourceConfig create(
        String jdbcDriverClass, String jdbcUrl, String dbUser, String dbPassword) {
      return new AutoValue_JdbcConnectionPools_DataSourceConfig(
          jdbcDriverClass, jdbcUrl, dbUser, dbPassword);
    }
  }
}
