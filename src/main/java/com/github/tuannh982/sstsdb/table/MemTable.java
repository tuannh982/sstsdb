package com.github.tuannh982.sstsdb.table;

import com.github.tuannh982.sstsdb.compact.Compactor;
import lombok.Getter;
import lombok.Setter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

@Getter
public class MemTable implements Table {
    private final Object lock = new Object[0];
    @Setter
    private TreeMap<String, DataEntry> map = new TreeMap<>();

    @Override
    public String get(String key) {
        synchronized (lock) {
            DataEntry entry = map.get(key);
            if (entry == null) return null;
            return entry.getValue();
        }
    }

    @Override
    public void put(String key, String value) {
        synchronized (lock) {
            DataEntry entry = map.get(key);
            if (entry == null) {
                map.put(key, new DataEntry(
                        System.currentTimeMillis(),
                        key,
                        value
                ));
            } else {
                entry.setTs(System.currentTimeMillis());
                entry.setValue(value);
            }
        }
    }

    public void writeToFile(String out) throws IOException {
        synchronized (lock) {
            try (BufferedWriter br = new BufferedWriter(new FileWriter(out))) {
                for (Map.Entry<String, DataEntry> entry : map.entrySet()) {
                    br.write(Compactor.OBJECT_MAPPER.writeValueAsString(entry.getValue()));
                    br.newLine();
                }
            }
        }
    }
}
