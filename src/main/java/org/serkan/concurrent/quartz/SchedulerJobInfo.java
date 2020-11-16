package org.serkan.concurrent.quartz;

import org.springframework.scheduling.quartz.QuartzJobBean;

public interface SchedulerJobInfo {

    Long getId();

    String getJobName();

    String getJobGroup();

    Class<? extends QuartzJobBean> getJobClass();

    String getCronExpression();

    Long getRepeatTime();

    Boolean getCronJob();
}