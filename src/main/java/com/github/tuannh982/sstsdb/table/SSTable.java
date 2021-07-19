package com.github.tuannh982.sstsdb.table;

import com.github.tuannh982.sstsdb.compact.Compactor;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.io.*;

@Getter
public class SSTable implements Table {
    private final Object lock = new Object[0];
    private final String file;
    private final Compactor.Tier tier;
    private final File f;

    public SSTable(String file, Compactor.Tier tier) throws IOException {
        this.file = file;
        this.tier = tier;
        f = new File(file);
        if (!f.exists()) {
            f.createNewFile();
        }
    }

    @Override
    public String get(String key) throws IOException {
        synchronized (lock) {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String entryStr = null;
                DataEntry entry = null;
                while (true) {
                    entryStr = reader.readLine();
                    if (entryStr == null) break;
                    entry = Compactor.OBJECT_MAPPER.readValue(entryStr, DataEntry.class);
                    if (StringUtils.equals(entry.getKey(), key)) {
                        return entry.getValue();
                    }
                }
            }
            return null;
        }
    }

    @Override
    public void put(String key, String value) {
        throw new UnsupportedOperationException();
    }
}
