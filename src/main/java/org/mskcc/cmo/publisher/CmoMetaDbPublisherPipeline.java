package org.mskcc.cmo.publisher;

import java.util.Date;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;
import org.mskcc.cmo.publisher.pipeline.config.BatchConfiguration;
import org.mskcc.cmo.publisher.pipeline.limsrest.LimsRequestUtil;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
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
        JobParametersBuilder jobParamsBuilder = new JobParametersBuilder();
        if (commandLine.hasOption("r") || commandLine.hasOption("s")) {
            // validatate format for start date and end date (if applicable)
            if (commandLine.hasOption("s")) {
                validateProvidedDates(commandLine.getOptionValue("s"), commandLine.getOptionValue("e"));
            }
            // set up job params for lims request publisher job
            jobName = BatchConfiguration.LIMS_REQUEST_PUBLISHER_JOB;
            jobParamsBuilder.addString("requestIds", commandLine.getOptionValue("r"))
                .addString("startDate", commandLine.getOptionValue("s"))
                .addString("endDate", commandLine.getOptionValue("e"));
        } else if (commandLine.hasOption("f")) {
            jobName = BatchConfiguration.METADB_FILE_PUBLISHER_JOB;
            jobParamsBuilder.addString("publisherFilename", commandLine.getOptionValue("f"));
        }

        // set up job, job launcher, and job execution
        JobLauncher jobLauncher = ctx.getBean(JobLauncher.class);
        Job job = ctx.getBean(jobName, Job.class);
        JobExecution jobExecution = jobLauncher.run(job, jobParamsBuilder.toJobParameters());
        if (!jobExecution.getExitStatus().equals(ExitStatus.COMPLETED)) {
            LOG.error(jobName + " failed with exit status: " + jobExecution.getExitStatus());
        } else {
            LOG.info("Job completed with exit status: '" + jobExecution.getExitStatus().getExitCode()
                    + "' - exiting application");
        }
        System.exit(SpringApplication.exit(ctx));
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

    private static Options getOptions(String[] args) {
        Options options = new Options();
        options.addOption("h", "help", false, "shows this help document and quits.")
                .addOption("r", "request_ids", true, "Comma-separated list of request ids to fetch "
                + "data for from LimsRest [REQUEST IDS MODE]")
                .addOption("s", "start_date", true, "Start date [YYYY/MM/DD], fetch requests from "
                        + "LimsRest beginning from the given start date [START/END DATE MODE]")
                .addOption("e", "end_date", true, "End date [YYYY/MM/DD]. Fetch requests from LimsRest "
                        + "between the start and end dates provided. [OPTIONAL, START/END DATE MODE]")
                .addOption("f", "publisher_filename", true, "Input publisher filename [FILE READING MODE]");
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
                || (!commandLine.hasOption("r")
                && !commandLine.hasOption("s") && !commandLine.hasOption("f"))) {
            help(options, 0);
        }
        // check that command line options entered are valid
        if (commandLine.hasOption("r") && (commandLine.hasOption("s")
                || commandLine.hasOption("e"))) {
            LOG.error("Cannot use '--request_ids with '--start_date' or '--end_date'");
            help(options, 1);
        } else if (commandLine.hasOption("f") && (commandLine.hasOption("r")
                || (commandLine.hasOption("s") || commandLine.hasOption("e")))) {
            LOG.error("Cannot use '--publisher_filename' with '--request_ids' or"
                    + "'--start_date | --end_date'");
            help(options, 1);
        } else if (!commandLine.hasOption("h") && !commandLine.hasOption("s")
                && !commandLine.hasOption("f") && !commandLine.hasOption("r")) {
            LOG.error("Must run application with at least option '-r', '-s', or '-f' - exiting...");
            help(options, 1);
        }
        return commandLine;
    }

}
