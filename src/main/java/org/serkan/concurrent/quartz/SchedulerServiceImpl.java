package org.serkan.concurrent.quartz;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.quartz.CronTriggerFactoryBean;
import org.springframework.scheduling.quartz.JobDetailFactoryBean;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.scheduling.quartz.SimpleTriggerFactoryBean;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.util.Date;

@Service
public class SchedulerServiceImpl implements SchedulerService {

    private static Log logger = LogFactory.getLog(SchedulerServiceImpl.class);

    @Autowired
    private SchedulerFactoryBean schedulerFactoryBean;

    @Autowired
    private ApplicationContext context;

    private Class<? extends QuartzJobBean> jobClass;

    private String jobName;

    private String jobGroup;

    @Override
    public void scheduleNewJob(SchedulerJobInfo jobInfo) {
        try {
            Scheduler scheduler = schedulerFactoryBean.getScheduler();
            jobClass = jobInfo.getJobClass();
            jobName = jobInfo.getJobName();
            jobGroup = jobInfo.getJobGroup();
            JobDetail jobDetail = JobBuilder.newJob(jobClass).withIdentity(jobName, jobGroup).build();
            if (!scheduler.checkExists(jobDetail.getKey())) {
                jobDetail = createJob(jobClass, false, context, jobName, jobGroup);
                scheduler.scheduleJob(jobDetail, createTrigger(jobInfo));
            } else {
                logger.error("scheduleNewJobRequest.jobAlreadyExist");
            }
        } catch (SchedulerException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void updateScheduleJob(SchedulerJobInfo jobInfo) {
        try {
            schedulerFactoryBean.getScheduler().rescheduleJob(TriggerKey.triggerKey(jobInfo.getJobName()),
                    createTrigger(jobInfo));
        } catch (SchedulerException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public boolean unScheduleJob(String jobName) {
        try {
            return schedulerFactoryBean.getScheduler().unscheduleJob(new TriggerKey(jobName));
        } catch (SchedulerException e) {
            logger.error("Failed to un-schedule job - " + jobName, e);
            return false;
        }
    }

    @Override
    public boolean deleteJob(SchedulerJobInfo jobInfo) {
        try {
            return schedulerFactoryBean.getScheduler()
                    .deleteJob(new JobKey(jobInfo.getJobName(), jobInfo.getJobGroup()));
        } catch (SchedulerException e) {
            logger.error("Failed to delete job - " + jobInfo.getJobName(), e);
            return false;
        }
    }

    @Override
    public boolean pauseJob(SchedulerJobInfo jobInfo) {
        try {
            schedulerFactoryBean.getScheduler().pauseJob(new JobKey(jobInfo.getJobName(), jobInfo.getJobGroup()));
            return true;
        } catch (SchedulerException e) {
            logger.error("Failed to pause job - " + jobInfo.getJobName(), e);
            return false;
        }
    }

    @Override
    public boolean resumeJob(SchedulerJobInfo jobInfo) {
        try {
            schedulerFactoryBean.getScheduler().resumeJob(new JobKey(jobInfo.getJobName(), jobInfo.getJobGroup()));
            return true;
        } catch (SchedulerException e) {
            logger.error("Failed to resume job - " + jobInfo.getJobName(), e);
            return false;
        }
    }

    @Override
    public boolean startJobNow(SchedulerJobInfo jobInfo) {
        try {
            schedulerFactoryBean.getScheduler().triggerJob(new JobKey(jobInfo.getJobName(), jobInfo.getJobGroup()));
            return true;
        } catch (SchedulerException e) {
            logger.error("Failed to start new job - " + jobInfo.getJobName(), e);
            return false;
        }
    }

    private Trigger createTrigger(SchedulerJobInfo jobInfo) {
        Trigger newTrigger;
        if (jobInfo.getCronJob()) {
            newTrigger = createCronTrigger(jobInfo.getJobName(), new Date(), jobInfo.getCronExpression(),
                    SimpleTrigger.MISFIRE_INSTRUCTION_FIRE_NOW);
        } else {
            newTrigger = createSimpleTrigger(jobInfo.getJobName(), new Date(), jobInfo.getRepeatTime(),
                    SimpleTrigger.MISFIRE_INSTRUCTION_FIRE_NOW);
        }
        return newTrigger;
    }

    /**
     * Create Quartz Job.
     *
     * @param jobClass  Class whose executeInternal() method needs to be called.
     * @param isDurable Job needs to be persisted even after completion. if true,
     *                  job will be persisted, not otherwise.
     * @param context   Spring application context.
     * @param jobName   Job name.
     * @param jobGroup  Job group.
     * @return JobDetail object
     */
    private JobDetail createJob(Class<? extends QuartzJobBean> jobClass, boolean isDurable, ApplicationContext context,
            String jobName, String jobGroup) {
        JobDetailFactoryBean factoryBean = new JobDetailFactoryBean();
        factoryBean.setJobClass(jobClass);
        factoryBean.setDurability(isDurable);
        factoryBean.setApplicationContext(context);
        factoryBean.setName(jobName);
        factoryBean.setGroup(jobGroup);

        // set job data map
        JobDataMap jobDataMap = new JobDataMap();
        jobDataMap.put(jobName + jobGroup, jobClass.getName());
        factoryBean.setJobDataMap(jobDataMap);

        factoryBean.afterPropertiesSet();

        return factoryBean.getObject();
    }

    /**
     * Create cron trigger.
     *
     * @param triggerName        Trigger name.
     * @param startTime          Trigger start time.
     * @param cronExpression     Cron expression.
     * @param misFireInstruction Misfire instruction (what to do in case of misfire
     *                           happens).
     * @return {@link CronTrigger}
     */
    private CronTrigger createCronTrigger(String triggerName, Date startTime, String cronExpression,
            int misFireInstruction) {
        CronTriggerFactoryBean factoryBean = new CronTriggerFactoryBean();
        factoryBean.setName(triggerName);
        factoryBean.setStartTime(startTime);
        factoryBean.setCronExpression(cronExpression);
        factoryBean.setMisfireInstruction(misFireInstruction);
        try {
            factoryBean.afterPropertiesSet();
        } catch (ParseException e) {
            logger.error(e.getMessage(), e);
        }
        return factoryBean.getObject();
    }

    /**
     * Create simple trigger.
     *
     * @param triggerName        Trigger name.
     * @param startTime          Trigger start time.
     * @param repeatTime         Job repeat period mills
     * @param misFireInstruction Misfire instruction (what to do in case of misfire
     *                           happens).
     * @return {@link SimpleTrigger}
     */
    private SimpleTrigger createSimpleTrigger(String triggerName, Date startTime, Long repeatTime,
            int misFireInstruction) {
        SimpleTriggerFactoryBean factoryBean = new SimpleTriggerFactoryBean();
        factoryBean.setName(triggerName);
        factoryBean.setStartTime(startTime);
        factoryBean.setRepeatInterval(repeatTime);
        factoryBean.setRepeatCount(SimpleTrigger.REPEAT_INDEFINITELY);
        factoryBean.setMisfireInstruction(misFireInstruction);
        factoryBean.afterPropertiesSet();
        return factoryBean.getObject();
    }
}