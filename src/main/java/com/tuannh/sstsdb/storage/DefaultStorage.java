package com.tuannh.sstsdb.storage;

import com.tuannh.sstsdb.compact.Scheduler;
import com.tuannh.sstsdb.compact.Compactor;
import com.tuannh.sstsdb.table.MemTable;
import com.tuannh.sstsdb.table.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Getter
public class DefaultStorage implements Table, Closeable {
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");

    private final String baseDir;
    @Setter
    private Compactor compactor;
    private final Object lock = new Object[0];
    @Setter
    private MemTable memTable;
    private final org.quartz.Scheduler scheduler;

    private final AtomicInteger pendingTask = new AtomicInteger(0);
    private final Object monitor = new Object[0];

    public DefaultStorage(String baseDir) throws SchedulerException, IOException {
        this.baseDir = baseDir;
        this.memTable = new MemTable();
        this.compactor = new Compactor(this.baseDir + "/" + DATE_FORMAT.format(new Date()), this.memTable);
        this.scheduler = new StdSchedulerFactory().getScheduler();
        Map<String, Object> dataMap = new HashMap<>();
        dataMap.put("storage", this);
        JobDataMap jobDataMap = new JobDataMap(dataMap);
        Trigger trigger = Scheduler.DAILY_TRIGGER.usingJobData(jobDataMap).build();
        JobDetail job = JobBuilder.newJob(DailyCompact.class).build();
        scheduler.start();
        scheduler.scheduleJob(job, trigger);
    }

    @Override
    public String get(String key) throws IOException {
        synchronized (lock) {
            return memTable.get(key);
        }
    }

    @Override
    public void put(String key, String value) throws IOException {
        synchronized (lock) {
            memTable.put(key, value);
        }
    }

    @SneakyThrows
    @Override
    public void close() throws IOException {
        compactor.close();
        compactor.memTblCompact();
        synchronized (monitor) {
            while (pendingTask.get() != 0) {
                monitor.wait(500);
            }
        }
        scheduler.shutdown(false);
    }

    public static class DailyCompact implements Job {
        @SneakyThrows
        @Override
        public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
            DefaultStorage defaultStorage = (DefaultStorage) jobExecutionContext.getMergedJobDataMap().get("storage");
            try {
                defaultStorage.getPendingTask().incrementAndGet();
                synchronized (defaultStorage.getLock()) {
                    Compactor compactor = defaultStorage.getCompactor();
                    synchronized (compactor.getLock()) {
                        compactor.close();
                        compactor.memTblCompact();
                    }
                    defaultStorage.setMemTable(new MemTable());
                    Date date = new Date();
                    defaultStorage.setCompactor(new Compactor(defaultStorage.getBaseDir() + "/" + DATE_FORMAT.format(date), defaultStorage.getMemTable()));
                }
            } finally {
                defaultStorage.getPendingTask().decrementAndGet();
            }
        }
    }
}
