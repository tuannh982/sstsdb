package com.github.tuannh982.sstsdb.compact;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.quartz.*;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Scheduler {
    public static final TriggerBuilder<?> TWO_SEC_TRIGGER = TriggerBuilder
            .newTrigger()
            .withSchedule(
                    SimpleScheduleBuilder.simpleSchedule()
                            .withIntervalInSeconds(2)
                            .repeatForever()
            );

    public static final TriggerBuilder<?> FIVE_MIN_TRIGGER = TriggerBuilder
            .newTrigger()
            .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                    .withIntervalInMinutes(5)
                    .repeatForever());

    public static final TriggerBuilder<?> THIRTY_MIN_TRIGGER = TriggerBuilder
            .newTrigger()
            .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                    .withIntervalInMinutes(30)
                    .repeatForever());

    public static final TriggerBuilder<?> HOURLY_TRIGGER = TriggerBuilder
            .newTrigger()
            .withSchedule(
                    SimpleScheduleBuilder.simpleSchedule()
                            .withIntervalInHours(1)
                            .repeatForever()
            );

    public static final TriggerBuilder<?> THREE_HOUR_TRIGGER = TriggerBuilder
            .newTrigger()
            .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                    .withIntervalInHours(3)
                    .repeatForever());

    public static final TriggerBuilder<?> TWELVE_HOUR_TRIGGER = TriggerBuilder
            .newTrigger()
            .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                    .withIntervalInHours(12)
                    .repeatForever()
            );

    public static final TriggerBuilder<?> DAILY_TRIGGER = TriggerBuilder
            .newTrigger()
            .withSchedule(
                    SimpleScheduleBuilder.simpleSchedule()
                            .withIntervalInHours(24)
                            .repeatForever()
            );

}
