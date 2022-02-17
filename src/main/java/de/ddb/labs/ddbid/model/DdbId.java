package de.ddb.labs.ddbid.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.sql.Timestamp;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class DdbId {

    @JsonFormat(pattern = "yyyy-MM-dd")
    private Timestamp timestamp;
    private String id;
    private String status;
    private String dataset_id;
    private String label;
    private String provider_id;
    private String supplier_id;

}
