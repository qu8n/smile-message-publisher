package org.mskcc.cmo.publisher.pipeline;

import org.apache.log4j.Logger;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

/**
 *
 * @author ochoaa
 */
public class MetaDbFilePublisherListener implements StepExecutionListener {
    private final Logger LOG = Logger.getLogger(MetaDbFilePublisherListener.class);

    @Override
    public void beforeStep(StepExecution se) {}

    @Override
    public ExitStatus afterStep(StepExecution se) {
        LOG.info("Finished publishing messages from input file...returning exit"
                + " status 'COMPLETED'");
        return ExitStatus.COMPLETED;
    }

}
