package de.ddb.labs.ddbid.model.paging;

import lombok.*;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class Order {

    private Integer column;
    private Direction dir;

}
