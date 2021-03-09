package com.tuannh.sstsdb.table;

import java.io.IOException;

public interface Table {
    String get(String key) throws IOException;
    void put(String key, String value) throws IOException;
}
