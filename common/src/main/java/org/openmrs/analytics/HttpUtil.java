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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpUtil {

	private static final Logger log = LoggerFactory.getLogger(FhirStoreUtil.class);

	private int maxRetries;

	private int retryInterval;

	private int timeout;

	HttpUtil(int maxRetries, int retryInterval, int timeout) {
		this.maxRetries = maxRetries;
		this.retryInterval = retryInterval;
		this.timeout = timeout;
	}

	public String executeRequest(HttpUriRequest request) throws IOException {

		RequestConfig config = RequestConfig.custom().setConnectTimeout(timeout * 1000)
				.setConnectionRequestTimeout(timeout * 1000).setSocketTimeout(timeout * 1000).build();

		HttpRequestRetryHandler requestRetryHandler = new HttpRequestRetryHandler() {

			@Override
			public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
				if (executionCount > maxRetries) {
					return false;
				} else {
					try {
						Thread.sleep(retryInterval * 1000);
					} catch (InterruptedException ex) {

					}
					return true;
				}
			}
		};

		CloseableHttpClient httpClient = HttpClientBuilder.create().setRetryHandler(requestRetryHandler)
				.setDefaultRequestConfig(config).build();

		try {
			// Execute the request and process the results.
			HttpResponse response = httpClient.execute(request);
			HttpEntity responseEntity = response.getEntity();
			ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
			responseEntity.writeTo(byteStream);
			if (response.getStatusLine().getStatusCode() != HttpStatus.SC_CREATED
			        && response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
				log.error(String.format("Exception for resource %s: %s", request.getURI().toString(),
				    response.getStatusLine().toString()));
				log.error(byteStream.toString());
				throw new RuntimeException();
			}
			return byteStream.toString();
		} catch (IOException e) {
			log.error("Error in opening url: " + request.getURI().toString() + " exception: " + e);
			return "";
		} finally {
			httpClient.close();
		}
	}

	public HttpURLConnection getServerConnection(String Serverurl) throws Exception {
		URL obj = new URL(Serverurl);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();
		con.setRequestMethod("GET");
		con.setRequestProperty("User-Agent", "Mozilla/5.0");
		return con;
	}

	public boolean checkServerConnection(String url) throws Exception {
		Integer connectionStatus = null;
		try {
			connectionStatus = getServerConnection(url).getResponseCode();
		} catch (Exception e) {
			connectionStatus = 0;
			log.error("Connection failed", e);
			return false;
		}
		if (connectionStatus != 404 && connectionStatus != null) {
			return true;
		} else {
			return false;
		}
	}
}
