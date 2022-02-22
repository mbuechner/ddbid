package de.ddb.labs.ddbid.controller;

import de.ddb.labs.ddbid.model.DdbId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import de.ddb.labs.ddbid.model.paging.Page;
import de.ddb.labs.ddbid.model.paging.PagingRequest;
import de.ddb.labs.ddbid.service.DdbIdService;
import java.sql.Timestamp;
import java.util.Map;

@RestController
@RequestMapping("item")
public class DdbIdRestController {

    private final DdbIdService ddbIdService;

    @Autowired
    public DdbIdRestController(DdbIdService ddbIdService) {
        this.ddbIdService = ddbIdService;
    }

    @PostMapping
    public Page<DdbId> list(@RequestBody PagingRequest pagingRequest) {
        return ddbIdService.getDdbIds(pagingRequest);
    }

    @PostMapping
    @RequestMapping("timestamp")
    public Map<String, Timestamp> timestamps() {
        return ddbIdService.getTimestamps();
    }
}
