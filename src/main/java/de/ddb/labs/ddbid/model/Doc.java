/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package de.ddb.labs.ddbid.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.Data;
import lombok.Getter;

/**
 *
 * @author michael
 */
@Data
public class Doc {

    public enum Status {
        NEW("NEW"), MISSING("MISSING");

        @Getter
        private final String status;

        private Status(String status) {
            this.status = status;
        }
    }

    @JsonIgnore
    private Timestamp timestamp;
    private String id;
    @JsonIgnore
    private Status status;
    private String dataset_id;
    private String[] label;
    private String[] provider_id;
    private String supplier_id;

    public static List<String> getHeader() {
        final List<String> l = new ArrayList<>();
        l.add("timestamp");
        l.add("id");
        l.add("status");
        l.add("dataset_id");
        l.add("label");
        l.add("provider_id");
        l.add("supplier_id");
        return l;
    }

    public List<Object> getData(Timestamp timestamp, Status status) {
        setTimestamp(timestamp);
        setStatus(status);
        return getData();
    }

    public List<Object> getData() {
        final List<Object> l = new ArrayList<>();
        l.add(getTimestamp());
        l.add(getId());
        l.add(getStatus());
        l.add(getDataset_id());
        l.add(Arrays.toString(getLabel()));
        l.add(Arrays.toString(getProvider_id()));
        l.add(getSupplier_id());
        return l;
    }
}
