package de.ddb.labs.ddbid.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping({ "/item" })
public class DdbIdController {

    @GetMapping
    public String main() {
        return "item";
    }
}