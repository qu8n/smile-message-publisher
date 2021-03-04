package org.mskcc.cmo.publisher;

import java.util.Date;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;
import org.mskcc.cmo.publisher.pipeline.BatchConfiguration;
import org.mskcc.cmo.publisher.pipeline.limsrest.LimsRequestUtil;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 *
 * @author DivyaMadala
 */
@SpringBootApplication
public class CmoMetaDbPublisherPipeline {

    private static final Logger LOG = Logger.getLogger(CmoMetaDbPublisherPipeline.class);

    public static void main(String[] args) throws Exception {
        SpringApplication app = new SpringApplication(CmoMetaDbPublisherPipeline.class);
        ConfigurableApplicationContext ctx = app.run(args);
        CommandLine commandLine = parseArgs(args);

        String jobName = null;
        JobExecution jobExecution = null;
        if (commandLine.hasOption("r") || commandLine.hasOption("s")) {
            // validatate format for start date and end date (if applicable)
            if (commandLine.hasOption("s")) {
                validateProvidedDates(commandLine.getOptionValue("s"), commandLine.getOptionValue("e"));
            }
            jobName = BatchConfiguration.LIMS_REQUEST_PUBLISHER_JOB;
            jobExecution = launchLimsRequestPublisherJob(ctx, commandLine.getOptionValue("r"),
                    commandLine.getOptionValue("s"), commandLine.getOptionValue("e"));
        } else {
            LOG.error("Must run application with at least option '-r' or '-s' - exiting...");
            System.exit(1);
        }
        if (!jobExecution.getExitStatus().equals(ExitStatus.COMPLETED)) {
            LOG.error(jobName + " failed with exit status: " + jobExecution.getExitStatus());
            System.exit(1);
        }
    }


    /**
     * Validate the start and end dates provided if applicable.
     * @param startDate
     * @param endDate
     */
    private static void validateProvidedDates(String startDate, String endDate) {
        Date startTimestamp = null;
        Date endTimestamp = null;
        // parse start date
        try {
            startTimestamp = LimsRequestUtil.DATE_FORMAT.parse(startDate);
        } catch (java.text.ParseException ex) {
            LOG.error("Error parsing start date - must be provided in format: YYYY/MM/DD");
            System.exit(2);
        }
        // parse end date if provided
        if (endDate != null) {
            try {
                endTimestamp = LimsRequestUtil.DATE_FORMAT.parse(endDate);
            } catch (java.text.ParseException ex) {
                LOG.error("Error parsing end date - must be provided in format: YYYY/MM/DD");
                System.exit(2);
            }
            // also check that end timestamp occurs after start timestamp
            if (endTimestamp.before(startTimestamp)) {
                LOG.error("End date provided must occur after the start date provided.");
                System.exit(2);
            }
        }
    }

    private static JobExecution launchLimsRequestPublisherJob(ConfigurableApplicationContext ctx,
            String requestIds, String startDate, String endDate) throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("requestIds", requestIds)
                .addString("startDate", startDate)
                .addString("endDate", endDate)
                .toJobParameters();
        JobLauncher jobLauncher = ctx.getBean(JobLauncher.class);
        Job job = ctx.getBean(BatchConfiguration.LIMS_REQUEST_PUBLISHER_JOB, Job.class);
        return jobLauncher.run(job, jobParameters);
    }

    private static Options getOptions(String[] args) {
        Options options = new Options();
        options.addOption("h", "help", false, "shows this help document and quits.")
                .addOption("r", "request_ids", true, "Comma-separated list of request ids to fetch "
                + "data for from LimsRest")
                .addOption("s", "start_date", true, "Start date [YYYY/MM/DD], fetch requests from "
                        + "LimsRest beginning from the given start date")
                .addOption("e", "end_date", true, "End date [YYYY/MM/DD]. Fetch requests from LimsRest "
                        + "between the start and end dates provided. [OPTIONAL]");
        return options;
    }

    private static void help(Options options, int exitStatus) {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp("CmoNewRequestPublisher", options);
        System.exit(exitStatus);
    }

    private static CommandLine parseArgs(String[] args) throws Exception {
        Options options = getOptions(args);
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);
        if (commandLine.hasOption("h")
                || (!commandLine.hasOption("r") && !commandLine.hasOption("s"))) {
            help(options, 0);
        }
        if (commandLine.hasOption("r") && (commandLine.hasOption("s") || commandLine.hasOption("e"))) {
            LOG.error("Cannot use '--request_ids with '--start_date' or '--end_date'");
            help(options, 1);
        }
        return commandLine;
    }

}
