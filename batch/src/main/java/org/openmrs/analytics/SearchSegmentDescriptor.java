package org.openmrs.analytics;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

@DefaultCoder(SerializableCoder.class)
@AutoValue
abstract class SearchSegmentDescriptor implements Serializable {

  static SearchSegmentDescriptor create(String searchUrl, int pageOffset, int count,
      String jsessionId) {
    return new AutoValue_SearchSegmentDescriptor(searchUrl, pageOffset, count, jsessionId);
  }

  abstract String searchUrl();
  abstract int pageOffset();
  abstract int count();
  abstract String jsessionId();
}
