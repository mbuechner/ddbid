/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package de.ddb.labs.ddbid.model;

import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DdbIdRowMapper implements RowMapper<DdbId> {

    @Override
    public DdbId mapRow(ResultSet rs, int rowNum) throws SQLException {

        final DdbId ddbid = new DdbId();
        ddbid.setDataset_id(rs.getString("dataset_id"));
        ddbid.setId(rs.getString("id"));
        ddbid.setLabel(rs.getString("label"));
        ddbid.setProvider_id(rs.getString("provider_id"));
        ddbid.setStatus(rs.getString("status"));
        ddbid.setSupplier_id(rs.getString("supplier_id"));
        ddbid.setTimestamp(rs.getTimestamp("created_date"));

        return ddbid;

    }
}