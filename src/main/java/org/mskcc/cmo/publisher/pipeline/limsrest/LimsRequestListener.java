package org.mskcc.cmo.publisher.pipeline.limsrest;

import org.apache.log4j.Logger;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author ochoaa
 */
public class LimsRequestListener implements StepExecutionListener {
    @Autowired
    private LimsRequestUtil limsRestUtil;

    private final Logger LOG = Logger.getLogger(LimsRequestListener.class);

    @Override
    public void beforeStep(StepExecution se) {}

    @Override
    public ExitStatus afterStep(StepExecution se) {
        if (!limsRestUtil.getLimsRequestErrors().isEmpty()) {
            LOG.warn("Encountered errors while fetching from LimsRest - see report summary for details");
            limsRestUtil.printFailedRequestSamplesSummary();
        } else {
            LOG.info("No errors to report during fetch from LimsRest");
        }
        return ExitStatus.COMPLETED;
    }

}
