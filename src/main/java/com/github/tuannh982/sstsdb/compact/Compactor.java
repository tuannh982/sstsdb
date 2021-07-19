package com.github.tuannh982.sstsdb.compact;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tuannh982.sstsdb.table.DataEntry;
import com.github.tuannh982.sstsdb.table.MemTable;
import com.github.tuannh982.sstsdb.table.SSTable;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.io.*;
import java.util.*;
import java.util.Calendar;
import java.util.concurrent.atomic.AtomicInteger;

@Getter
public class Compactor implements Closeable {
        public enum Tier {
        HOUR, // [24]: 0..23
        THREE_HOUR, // [8]:  0-2, 3-5, 6-8, 9-11, 12-14, 15-17, 18-20, 21-23
        TWELVE_HOUR, // [2]: 0-11, 12-23
        DAY
    }
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @SuppressWarnings({"java:S2095", "java:S135", "java:S3776", "java:S4042", "java:S899"})
    public static void compact(final String sortedFile1, final String sortedFile2, final String outFile) {
        String tempOutFile = outFile;
        short mode = 0; // no overwrite
        if (StringUtils.equals(sortedFile1, outFile)) {
            mode = 1;
            tempOutFile += "temp";
        } else if (StringUtils.equals(sortedFile2, outFile)) {
            mode = 2;
            tempOutFile += "temp";
        }
        try {
            BufferedReader br1 = new BufferedReader(new FileReader(sortedFile1));
            BufferedReader br2 = new BufferedReader(new FileReader(sortedFile2));
            BufferedWriter out = new BufferedWriter(new FileWriter(tempOutFile));
            boolean continueF1 = true;
            boolean continueF2 = true;
            boolean nfb = true;
            DataEntry d1 = null;
            DataEntry d2 = null;
            String e1 = null;
            String e2 = null;
            while (true) {
                if (continueF1) {
                    e1 = br1.readLine();
                    if (e1 == null) {
                        break;
                    } else {
                        d1 = OBJECT_MAPPER.readValue(e1, DataEntry.class);
                    }
                }
                if (continueF2) {
                    e2 = br2.readLine();
                    if (e2 == null) {
                        break;
                    } else {
                        d2 = OBJECT_MAPPER.readValue(e2, DataEntry.class);
                    }
                }
                nfb = false;
                int cmpResult = d1.compareTo(d2);
                if (cmpResult < 0) {
                    out.write(OBJECT_MAPPER.writeValueAsString(d1));
                    out.newLine();
                    continueF1 = true;
                    continueF2 = false;
                } else if (cmpResult == 0) {
                    if (d1.getTs() > d2.getTs()) {
                        out.write(OBJECT_MAPPER.writeValueAsString(d1));
                        out.newLine();
                    } else {
                        out.write(OBJECT_MAPPER.writeValueAsString(d2));
                        out.newLine();
                    }
                    continueF1 = true;
                    continueF2 = true;
                } else {
                    out.write(OBJECT_MAPPER.writeValueAsString(d2));
                    out.newLine();
                    continueF1 = false;
                    continueF2 = true;
                }
            }
            if (continueF1) {
                if (nfb && d1 != null) {
                    out.write(OBJECT_MAPPER.writeValueAsString(d1));
                    out.newLine();
                }
                while (true) {
                    e1 = br1.readLine();
                    if (e1 == null) {
                        break;
                    } else {
                        d1 = OBJECT_MAPPER.readValue(e1, DataEntry.class);
                        out.write(OBJECT_MAPPER.writeValueAsString(d1));
                        out.newLine();
                    }
                }
            }
            if (continueF2) {
                if (nfb && d2 != null) {
                    out.write(OBJECT_MAPPER.writeValueAsString(d2));
                    out.newLine();
                }
                while (true) {
                    e2 = br2.readLine();
                    if (e2 == null) {
                        break;
                    } else {
                        d2 = OBJECT_MAPPER.readValue(e2, DataEntry.class);
                        out.write(OBJECT_MAPPER.writeValueAsString(d2));
                        out.newLine();
                    }
                }
            }
            br1.close();
            br2.close();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (mode == 1) {
            new File(sortedFile1).delete();
            new File(tempOutFile).renameTo(new File(sortedFile1));
        } else if (mode == 2) {
            new File(sortedFile2).delete();
            new File(tempOutFile).renameTo(new File(sortedFile2));
        }
    }

    private static final Calendar calendar = Calendar.getInstance();

    private final String tempDir;
    private final MemTable memTable;
    private final List<SSTable> hourlySsTable;
    private final List<SSTable> threeHourSsTable;
    private final List<SSTable> twelveHourSsTable;
    private final SSTable dailySsTable;
    private final org.quartz.Scheduler scheduler;
    private final Object lock = new Object[0];

    private final AtomicInteger pendingTask = new AtomicInteger(0);
    private final Object monitor = new Object[0];

    public Compactor(String ssDir, MemTable memTable) throws SchedulerException, IOException {
        new File(ssDir).mkdirs();
        this.tempDir = ssDir;
        this.memTable = memTable;
        //
        final String hourlyBaseFile = ssDir + "/h";
        hourlySsTable = new ArrayList<>();
        for (int i = 0; i < 24; i++) {
            final String fileName = hourlyBaseFile + i;
            hourlySsTable.add(new SSTable(fileName, Tier.HOUR));
        }
        //
        final String threeHourBaseFile = ssDir + "/3h";
        threeHourSsTable = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            final String fileName = threeHourBaseFile + i;
            threeHourSsTable.add(new SSTable(fileName, Tier.THREE_HOUR));
        }
        //
        final String twelveHourBaseFile = ssDir + "/12h";
        twelveHourSsTable = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            final String fileName = twelveHourBaseFile + i;
            twelveHourSsTable.add(new SSTable(fileName, Tier.TWELVE_HOUR));
        }
        //
        final String dailyFileName = ssDir + "/day";
        dailySsTable = new SSTable(dailyFileName, Tier.DAY);
        //
        scheduler = new StdSchedulerFactory().getScheduler();
        initJob(scheduler);
    }

    public Compactor(
            String ssDir,
            MemTable memTable,
            List<SSTable> hourlySsTable,
            List<SSTable> threeHourSsTable,
            List<SSTable> twelveHourSsTable,
            SSTable dailySsTable
    ) throws SchedulerException {
        this.tempDir = ssDir;
        this.memTable = memTable;
        this.hourlySsTable = hourlySsTable;
        this.threeHourSsTable = threeHourSsTable;
        this.twelveHourSsTable = twelveHourSsTable;
        this.dailySsTable = dailySsTable;
        //
        scheduler = new StdSchedulerFactory().getScheduler();
        initJob(scheduler);
    }

    private void initJob(org.quartz.Scheduler scheduler) throws SchedulerException {
        Map<String, Object> dataMap = new HashMap<>();
        dataMap.put("compactor", this);
        JobDataMap jobDataMap = new JobDataMap(dataMap);
        //
        Trigger trigger0 = com.github.tuannh982.sstsdb.compact.Scheduler.FIVE_MIN_TRIGGER.withIdentity("trigger0").usingJobData(jobDataMap).build();
        JobDetail job0 = JobBuilder.newJob(JobMemTblCompact.class).build();
        Trigger trigger1 = com.github.tuannh982.sstsdb.compact.Scheduler.THIRTY_MIN_TRIGGER.withIdentity("trigger1").usingJobData(jobDataMap).build();
        JobDetail job1 = JobBuilder.newJob(JobHourly2ThreeHourCompact.class).build();
        Trigger trigger2 = com.github.tuannh982.sstsdb.compact.Scheduler.HOURLY_TRIGGER.withIdentity("trigger2").usingJobData(jobDataMap).build();
        JobDetail job2 = JobBuilder.newJob(JobThreeHourCompact2TwelveHourCompact.class).build();
        Trigger trigger3 = com.github.tuannh982.sstsdb.compact.Scheduler.THREE_HOUR_TRIGGER.withIdentity("trigger3").usingJobData(jobDataMap).build();
        JobDetail job3 = JobBuilder.newJob(JobTwelveHour2DailyCompact.class).build();
        scheduler.scheduleJob(job0, trigger0);
        scheduler.scheduleJob(job1, trigger1);
        scheduler.scheduleJob(job2, trigger2);
        scheduler.scheduleJob(job3, trigger3);
        scheduler.start();
        // test same trigger
    }

    @SneakyThrows
    @Override
    public void close() throws IOException {
        synchronized (monitor) {
            while (pendingTask.get() != 0) {
                monitor.wait(500);
            }
        }
        scheduler.shutdown(false);
    }

    public void memTblCompact() throws IOException {
        pendingTask.incrementAndGet();
        try {
            final String tempTbl = tempDir + "/temptbl";
            int hour = calendar.get(Calendar.HOUR_OF_DAY);
            synchronized (lock) {
                File f = new File(tempTbl);
                f.delete();
                f.createNewFile();
                SSTable ssTable = hourlySsTable.get(hour);
                memTable.writeToFile(tempTbl);
                compact(ssTable.getFile(), tempTbl, ssTable.getFile());
            }
        } finally {
            pendingTask.decrementAndGet();
        }
    }

    public static class JobMemTblCompact implements Job {
        @SneakyThrows
        @Override
        public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
            Compactor compactor = (Compactor) jobExecutionContext.getMergedJobDataMap().get("compactor");
            try {
                compactor.getPendingTask().incrementAndGet();
                synchronized (compactor.getLock()) {
                    String tempDir = compactor.getTempDir();
                    MemTable memTable = compactor.getMemTable();
                    List<SSTable> hourlySsTable = compactor.getHourlySsTable();
                    final String tempTbl = tempDir + "/temptbl";
                    File f = new File(tempTbl);
                    f.delete();
                    f.createNewFile();
                    int hour = calendar.get(Calendar.HOUR_OF_DAY);
                    SSTable ssTable = hourlySsTable.get(hour);
                    memTable.writeToFile(tempTbl);
                    compact(ssTable.getFile(), tempTbl, ssTable.getFile());
                }
            } finally {
                compactor.getPendingTask().decrementAndGet();
            }
        }
    }

    public static class JobHourly2ThreeHourCompact implements Job {
        @Override
        public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
            Compactor compactor = (Compactor) jobExecutionContext.getMergedJobDataMap().get("compactor");
            try {
                compactor.getPendingTask().incrementAndGet();
                synchronized (compactor.getLock()) {
                    List<SSTable> hourlySsTable = compactor.getHourlySsTable();
                    List<SSTable> threeHourSsTable = compactor.getThreeHourSsTable();
                    int upper = 0;
                    for (int i = 0; i < 24; i++) {
                        upper = i / 3;
                        SSTable srcTbl = hourlySsTable.get(i);
                        SSTable dstTbl = threeHourSsTable.get(upper);
                        compact(dstTbl.getFile(), srcTbl.getFile(), dstTbl.getFile());
                    }
                }
            } finally {
                compactor.getPendingTask().decrementAndGet();
            }
        }
    }

    public static class JobThreeHourCompact2TwelveHourCompact implements Job {
        @Override
        public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
            Compactor compactor = (Compactor) jobExecutionContext.getMergedJobDataMap().get("compactor");
            try {
                compactor.getPendingTask().incrementAndGet();
                synchronized (compactor.getLock()) {
                    List<SSTable> threeHourSsTable = compactor.getThreeHourSsTable();
                    List<SSTable> twelveHourSsTable = compactor.getTwelveHourSsTable();
                    int upper = 0;
                    for (int i = 0; i < 8; i++) {
                        upper = i / 4;
                        SSTable srcTbl = threeHourSsTable.get(i);
                        SSTable dstTbl = twelveHourSsTable.get(upper);
                        compact(dstTbl.getFile(), srcTbl.getFile(), dstTbl.getFile());
                    }
                }
            } finally {
                compactor.getPendingTask().decrementAndGet();
            }
        }
    }

    public static class JobTwelveHour2DailyCompact implements Job {
        @Override
        public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
            Compactor compactor = (Compactor) jobExecutionContext.getMergedJobDataMap().get("compactor");
            try {
                compactor.getPendingTask().incrementAndGet();
                synchronized (compactor.getLock()) {
                    List<SSTable> twelveHourSsTable = compactor.getTwelveHourSsTable();
                    SSTable dailySsTable = compactor.getDailySsTable();
                    for (int i = 0; i < 2; i++) {
                        SSTable srcTbl = twelveHourSsTable.get(i);
                        SSTable dstTbl = dailySsTable;
                        compact(dstTbl.getFile(), srcTbl.getFile(), dstTbl.getFile());
                    }
                }
            } finally {
                compactor.getPendingTask().decrementAndGet();
            }
        }
    }

}
