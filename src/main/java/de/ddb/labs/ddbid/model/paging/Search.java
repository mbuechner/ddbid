package de.ddb.labs.ddbid.model.paging;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@NoArgsConstructor
@ToString
public class Search {

    private String value;
    private String regex;
}
