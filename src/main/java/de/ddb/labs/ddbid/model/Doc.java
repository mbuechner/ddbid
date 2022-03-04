/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package de.ddb.labs.ddbid.model;

import lombok.Data;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static de.ddb.labs.ddbid.model.item.ItemDoc.getStaticHeader;

/**
 * @author buechner
 */
@Data
public class Doc {

    public List<String> getHeader() {
        return getStaticHeader();
    }

    public List<Object> getData(Timestamp timestamp, Status status) {
        return getData();
    }

    public List<Object> getData() {
        return new ArrayList<>();
    }

}
