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
 */
package de.ddb.labs.ddbid.controller;

import de.ddb.labs.ddbid.database.Database;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

@Controller
@RequestMapping({"/statistics.html"})
public class StatisticsController {

    @Autowired
    private Database database;

    private final static String MISSING_BY_PROVIDER = "SELECT provider_id, count(id) AS MISSING FROM main.item \n"
            + "WHERE status = 'MISSING'\n"
            + "GROUP BY provider_id\n"
            + "ORDER BY count(id) DESC;";

    private final static String MISSING_BY_SECTOR = "SELECT sector_fct, count(id) AS MISSING FROM main.item \n"
            + "WHERE status = 'MISSING'\n"
            + "AND status != null\n"
            + "GROUP BY sector_fct\n"
            + "ORDER BY count(id) DESC;";

    private final static String QUERY_ITEM_MISSING = "SELECT \"timestamp\", count(id) AS COUNT\n"
            + "FROM main.item\n"
            + "WHERE status = 'MISSING'\n"
            + "GROUP BY \"timestamp\"\n"
            + "ORDER BY \"timestamp\" ASC;";

    private final static String QUERY_PERSON_MISSING = "SELECT \"timestamp\", count(id) AS COUNT\n"
            + "FROM main.person\n"
            + "WHERE status = 'MISSING'\n"
            + "GROUP BY \"timestamp\"\n"
            + "ORDER BY \"timestamp\" ASC;";

    private final static String QUERY_ORGANIZATION_MISSING = "SELECT \"timestamp\", count(id) AS COUNT\n"
            + "FROM main.organization\n"
            + "WHERE status = 'MISSING'\n"
            + "GROUP BY \"timestamp\"\n"
            + "ORDER BY \"timestamp\" ASC;";

    private final Calendar cal = Calendar.getInstance(Locale.GERMANY);
    private final DateTimeFormatter dtf = DateTimeFormatter.ISO_DATE;

    @GetMapping
    public ModelAndView main() {

        final ModelAndView mav = new ModelAndView();

        final Map<String, Integer> map = database.getJdbcTemplate().query(MISSING_BY_PROVIDER, new ResultSetExtractor<Map>() {
            @Override
            public Map extractData(ResultSet rs) throws SQLException, DataAccessException {
                Map<String, Integer> mapRet = new LinkedHashMap<>();
                while (rs.next()) {
                    mapRet.put(rs.getString("provider_id"), rs.getInt("MISSING"));
                }
                return mapRet;
            }
        });
        mav.addObject("missingByProvider_idKeys", new ArrayList<>(map.keySet()));
        mav.addObject("missingByProvider_idValues", new ArrayList<>(map.values()));

        final Map<String, Integer> mas = database.getJdbcTemplate().query(MISSING_BY_SECTOR, new ResultSetExtractor<Map>() {
            @Override
            public Map extractData(ResultSet rs) throws SQLException, DataAccessException {
                Map<String, Integer> mapRet = new LinkedHashMap<>();
                while (rs.next()) {
                    mapRet.put(rs.getString("sector_fct"), rs.getInt("MISSING"));
                }
                return mapRet;
            }
        });
        mav.addObject("missingBySector_fctKeys", new ArrayList<>(mas.keySet()));
        mav.addObject("missingBySector_fctValues", new ArrayList<>(mas.values()));

        final Map<Timestamp, Integer> mim = database.getJdbcTemplate().query(QUERY_ITEM_MISSING, new ResultSetExtractor<Map>() {
            @Override
            public Map extractData(ResultSet rs) throws SQLException, DataAccessException {
                Map<Timestamp, Integer> mapRet = new LinkedHashMap<>();
                while (rs.next()) {
                    mapRet.put(rs.getTimestamp("timestamp"), rs.getInt("COUNT"));
                }
                return mapRet;
            }
        });
        mav.addObject("itemMissingKeys", makeLabels(mim));
        mav.addObject("itemMissingValues", new ArrayList<>(mim.values()));

        final Map<Timestamp, Integer> mpm = database.getJdbcTemplate().query(QUERY_PERSON_MISSING, new ResultSetExtractor<Map>() {
            @Override
            public Map extractData(ResultSet rs) throws SQLException, DataAccessException {
                Map<Timestamp, Integer> mapRet = new LinkedHashMap<>();
                while (rs.next()) {
                    mapRet.put(rs.getTimestamp("timestamp"), rs.getInt("COUNT"));
                }
                return mapRet;
            }
        });
        mav.addObject("personMissingKeys", makeLabels(mpm));
        mav.addObject("personMissingValues", new ArrayList<>(mpm.values()));

        final Map<Timestamp, Integer> mom = database.getJdbcTemplate().query(QUERY_ORGANIZATION_MISSING, new ResultSetExtractor<Map>() {
            @Override
            public Map extractData(ResultSet rs) throws SQLException, DataAccessException {
                Map<Timestamp, Integer> mapRet = new LinkedHashMap<>();
                while (rs.next()) {
                    mapRet.put(rs.getTimestamp("timestamp"), rs.getInt("COUNT"));
                }
                return mapRet;
            }
        });
        mav.addObject("organizationMissingKeys", makeLabels(mom));
        mav.addObject("organizationMissingValues", new ArrayList<>(mom.values()));

        mav.setViewName("statistics");
        return mav;
    }

    private List<String> makeLabels(Map<Timestamp, Integer> ts) {
        final List<String> m = new ArrayList<>();
        for (Map.Entry<Timestamp, Integer> t : ts.entrySet()) {
            cal.setTime(t.getKey());
            m.add(dtf.format(t.getKey().toLocalDateTime()) + " (CW" + cal.get(Calendar.WEEK_OF_YEAR) + ")");
        }
        return m;
    }
}