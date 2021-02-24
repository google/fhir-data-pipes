
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

import java.beans.PropertyVetoException;
import java.sql.SQLException;

import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GetUuidUtilTest extends TestCase {
	
	@Mock
	private JdbcConnectionUtil jdbcConnectionUtil;
	
	@Mock
	private GetUuidUtil getUuidUtil;
	
	@Test
	public void shouldCallGetUuid() throws ClassNotFoundException, PropertyVetoException, SQLException {
		
		String uuid = getUuidUtil.getUuid(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
		
		lenient().when(getUuidUtil.getUuid(Mockito.anyString(), Mockito.anyString(), Mockito.anyString())).thenReturn(uuid);
		
		Mockito.verify(getUuidUtil, times(1)).getUuid(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
		Mockito.verify(getUuidUtil).getUuid(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
		
	}
}
