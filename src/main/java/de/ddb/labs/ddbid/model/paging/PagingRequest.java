package de.ddb.labs.ddbid.model.paging;

import java.util.List;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@ToString
@NoArgsConstructor
public class PagingRequest {

    private int start;
    private int length;
    private int draw;
    private List<Order> order;
    private List<Column> columns;
    private Search search;
    private Long timestamp;
    private String status;

}
