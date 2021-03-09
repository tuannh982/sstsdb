package com.tuannh.sstsdb.table;

import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@EqualsAndHashCode
@ToString
public class DataEntry implements Comparable<DataEntry> { // just testing
    private long ts;
    private @NonNull String key;
    private String value;

    @Override
    public int compareTo(DataEntry dataEntry) {
        return key.compareTo(dataEntry.getKey());
    }
}