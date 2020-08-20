package org.openmrs.analytics;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URLEncodedUtils;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Bundle.BundleLinkComponent;
import org.hl7.fhir.r4.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FhirSearchUtil {
  private static final Logger log = LoggerFactory.getLogger(FhirSearchUtil.class);

  private FhirContext fhirContext;
  private FhirStoreUtil fhirStoreUtil;

  FhirSearchUtil(FhirStoreUtil fhirStoreUtil, FhirContext fhirContext) {
    this.fhirStoreUtil = fhirStoreUtil;
    this.fhirContext = fhirContext;
  }

  Bundle searchForResource(String baseSearchUrl, int count, int first,
      String jsessionId, boolean parse, FhirContext fhirContext) {
    try {
      // TODO switch to HAPI FHIR client if all required features are supported.
      URIBuilder uriBuilder = new URIBuilder(baseSearchUrl);
      HttpUriRequest request = RequestBuilder
          .get()
          .setUri(uriBuilder.build())
          .addHeader("Content-Type", "application/fhir+json")
          .addHeader("Accept-Charset", "utf-8")
          .addHeader("Accept", "application/fhir+json")
          .addHeader("Cookie", "JSESSIONID=" + jsessionId)
          .addParameter("_count", String.valueOf(count))
          .addParameter("_getpagesoffset", String.valueOf(first))
          .addParameter("_summary", "data")
          .build();
      log.info("Fetching " + request.getURI());
      String bundleStr = fhirStoreUtil.executeRequest(request);
      if (parse) {
        IParser parser = fhirContext.newJsonParser();
        Bundle bundle = parser.parseResource(Bundle.class, bundleStr);
        log.info(String.format("Fetched %d resources out of a total of %d",
            bundle.getEntry().size(), bundle.getTotal()));
        return bundle;
      } else {
        return null;
      }
    } catch (URISyntaxException e) {
      log.error("Malformed FHIR url: " + baseSearchUrl + " Exception: " + e);
    }
    return null;
  }

  String findBaseSearchUrl(Bundle searchBundle) {
    String searchLink = null;
    for (BundleLinkComponent link : searchBundle.getLink()) {
      // TODO: Find and use FHIR constants.
      if (link.getRelation().equals("self") && searchLink == null) {
        searchLink = link.getUrl();
      }
      if (link.getRelation().equals("next")) {
        searchLink = link.getUrl();
        break;
      }
    }
    if (searchLink == null) {
      throw new IllegalArgumentException(
          String.format("No proper link information in bundle %s", searchBundle));
    }
    try {
      URI searchUri = new URI(searchLink);
      NameValuePair pagesParam = null;
      for (NameValuePair pair : URLEncodedUtils.parse(searchUri, StandardCharsets.UTF_8)) {
        if (pair.getName().equals("_getpages")) {
          pagesParam = pair;
        }
      }
      if (pagesParam == null) {
        throw new IllegalArgumentException(
            String.format("No _getpages parameter found in search link %s", searchLink));
      }
      return new URI(searchUri.getScheme(),
          searchUri.getAuthority(),
          searchUri.getPath(),
          pagesParam.toString(), // Only keep the _getpages parameter.
          searchUri.getFragment()).toString();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          String.format("Malformed link information with error %s in bundle %s", e.getMessage(),
              searchBundle));
    }
  }

  public void uploadBundleToCloud(Bundle bundle) {
    IParser parser = fhirContext.newJsonParser();
    for (BundleEntryComponent entry : bundle.getEntry()) {
      Resource resource = entry.getResource();
      fhirStoreUtil.uploadResourceToCloud(resource.getResourceType().name(),
          resource.getIdElement().getIdPart(), parser.encodeResourceToString(resource));
    }
  }

}
