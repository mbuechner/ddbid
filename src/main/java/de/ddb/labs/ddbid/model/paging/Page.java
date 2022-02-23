package de.ddb.labs.ddbid.model.paging;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;
import lombok.ToString;

@Getter
@Setter
@ToString
@NoArgsConstructor
public class Page<T> {

    public Page(List<T> data) {
        this.data = data;
    }

    private List<T> data;
    private int recordsFiltered;
    private int recordsTotal;
    private int draw;

}
