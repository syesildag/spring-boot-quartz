package org.serkan.concurrent.quartz.jobs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;

import java.util.stream.IntStream;

public class SimpleJob extends QuartzJobBean {

    private static Log logger = LogFactory.getLog(SimpleJob.class);

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        logger.info("SimpleJob Start................");
        IntStream.range(0, 10).allMatch(i -> {
            logger.info("Counting - " + i);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error(e.getMessage(), e);
                return false;
            }
            return true;
        });
        logger.info("SimpleJob End................");
    }
}