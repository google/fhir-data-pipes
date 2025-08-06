package com.google.fhir.analytics;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class DwhDto {
    @JsonProperty("dwh_prefix")
    private String dwhPrefix;

    @JsonProperty("dwh_path_latest")
    private String dwhPath;

    @JsonProperty("dwh_snapshots")
    private List<String> dwhSnapshots;

}
