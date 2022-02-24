/* 
 * Copyright 2022 Michael Büchner, Deutsche Digitale Bibliothek
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */package de.ddb.labs.ddbid.model;

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