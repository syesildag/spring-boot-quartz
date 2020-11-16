package org.serkan.concurrent.runners;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.serkan.concurrent.quartz.SchedulerJobInfo;
import org.serkan.concurrent.quartz.SchedulerService;
import org.serkan.concurrent.quartz.jobs.SimpleJob;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

@Order(Ordered.HIGHEST_PRECEDENCE)
@Component
public class QuartzSchedulerRunner implements ApplicationRunner, DisposableBean {

    private static Log logger = LogFactory.getLog(QuartzSchedulerRunner.class);

    @Autowired
    private SchedulerService schedulerService;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        logger.info("Schedule all new scheduler jobs at app startup - starting");

        SchedulerJobInfo info = new SchedulerJobInfo() {

			@Override
			public Long getId() {
				return 1L;
			}

			@Override
			public String getJobName() {
				return "jobName";
			}

			@Override
			public String getJobGroup() {
				return "jobGroup";
			}

			@Override
			public Class<? extends QuartzJobBean> getJobClass() {
				return SimpleJob.class;
			}

			@Override
			public String getCronExpression() {
				return "jobCronExpression";
			}

			@Override
			public Long getRepeatTime() {
				return 60000L;
			}

			@Override
			public Boolean getCronJob() {
				return false;
			}
        };

        try {
            schedulerService.scheduleNewJob(info);
            logger.info("Schedule all new scheduler jobs at app startup - complete");
        } catch (Exception ex) {
            logger.error("Schedule all new scheduler jobs at app startup - error", ex);
        }
    }

    @Override
    public void destroy() throws Exception {
        logger.debug("Schedule all new scheduler jobs at app startup - done");
    }
}