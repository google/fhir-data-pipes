package com.google.fhir.analytics;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class ScheduleDto {
    @JsonProperty("next_run")
    private String nextRun;
}
