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

@RestController
@RequestMapping("ddbid")
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
}
