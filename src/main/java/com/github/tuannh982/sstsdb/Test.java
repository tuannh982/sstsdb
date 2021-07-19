package com.github.tuannh982.sstsdb;

import com.github.tuannh982.sstsdb.compact.Compactor;
import com.github.tuannh982.sstsdb.storage.DefaultStorage;
import com.github.tuannh982.sstsdb.table.SSTable;
import com.github.tuannh982.sstsdb.table.Table;
import org.quartz.*;

import java.io.IOException;

public class Test {

    public static void test1() {
        // test
        String f1 = "/home/tuannh/Desktop/work/test-log/f1";
        String f2 = "/home/tuannh/Desktop/work/test-log/f2";
        String out = "/home/tuannh/Desktop/work/test-log/out";
        Compactor.compact(f1, f2, f1);
    }

    public static void test2() throws IOException {
        String f = "/home/tuannh/Desktop/work/test-log/f1";
        Table tbl = new SSTable(f, Compactor.Tier.HOUR);
        System.out.println(tbl.get("A"));
        System.out.println(tbl.get("B"));
        System.out.println(tbl.get("C"));
        System.out.println(tbl.get("D"));
        System.out.println(tbl.get("E"));
    }

    public static void test3() throws IOException, SchedulerException, InterruptedException {
        DefaultStorage defaultStorage = new DefaultStorage("/home/tuannh/Desktop/work/test-log");
        defaultStorage.put("A", "B");
        defaultStorage.put("A", "E");
        defaultStorage.put("B", "C");
        defaultStorage.put("C", "X");
        defaultStorage.close();
    }

    public static void main(String[] args) throws Exception {
        test2();
    }
}
