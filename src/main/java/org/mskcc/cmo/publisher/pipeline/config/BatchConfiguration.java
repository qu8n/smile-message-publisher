package org.mskcc.cmo.publisher.pipeline.config;

import java.util.Map;
import java.util.concurrent.Future;
import org.mskcc.cmo.messaging.Gateway;
import org.mskcc.cmo.publisher.pipeline.MetaDbFilePublisherListener;
import org.mskcc.cmo.publisher.pipeline.MetaDbFilePublisherReader;
import org.mskcc.cmo.publisher.pipeline.MetaDbFilePublisherWriter;
import org.mskcc.cmo.publisher.pipeline.limsrest.LimsRequestListener;
import org.mskcc.cmo.publisher.pipeline.limsrest.LimsRequestProcessor;
import org.mskcc.cmo.publisher.pipeline.limsrest.LimsRequestReader;
import org.mskcc.cmo.publisher.pipeline.limsrest.LimsRequestWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 *
 * @author ochoaa
 */
@Configuration
@EnableBatchProcessing
@EnableAsync
@ComponentScan(basePackages = "org.mskcc.cmo.messaging")
public class BatchConfiguration {

    public static final String LIMS_REQUEST_PUBLISHER_JOB = "limsRequestPublisherJob";
    public static final String METADB_FILE_PUBLISHER_JOB = "metadbFilePublisherJob";

    @Value("${chunk.interval:10}")
    private Integer chunkInterval;

    @Value("${async.thread_pool_size:5}")
    private Integer asyncThreadPoolSize;

    @Value("${async.thread_pool_max:10}")
    private Integer asyncThreadPoolMax;

    @Value("${processor.thread_pool_size:5}")
    private Integer processorThreadPoolSize;

    @Value("${processor.thread_pool_max:10}")
    private Integer processorThreadPoolMax;

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    private Gateway messagingGateway;

    @Autowired
    public void initMessagingGateway() throws Exception {
        messagingGateway.connect();
    }

    /**
     * limsRequestPublisherJob
     * @return
     */
    @Bean
    public Job limsRequestPublisherJob() {
        return jobBuilderFactory.get(LIMS_REQUEST_PUBLISHER_JOB)
                .start(limsRequestPublisherStep())
                .build();
    }

    /**
     * metadbFilePublisherJob
     * @return
     */
    @Bean
    public Job metadbFilePublisherJob() {
        return jobBuilderFactory.get(METADB_FILE_PUBLISHER_JOB)
                .start(metadbFilePublisherStep())
                .build();
    }

    /**
     * limsRequestPublisherStep
     * @return
     */
    @Bean
    public Step limsRequestPublisherStep() {
        return stepBuilderFactory.get("limsRequestPublisherStep")
                .listener(limsRequestListener())
                .<String, Future<Map<String,Object>>>chunk(chunkInterval)
                .reader(limsRequestReader())
                .processor(asyncItemProcessor())
                .writer(asyncItemWriter())
                .build();
    }

    /**
     * metadbFilePublisherStep
     * @return
     */
    @Bean
    public Step metadbFilePublisherStep() {
        return stepBuilderFactory.get("metadbFilePublisherStep")
                .listener(metadbFilePublisherListener())
                .<Map<String, String>, Map<String, String>>chunk(10)
                .reader(metadbFilePublisherReader())
                .writer(metadbFilePublisherWriter())
                .build();
    }

    /**
     * metadbFilePublisherReader
     * @return
     */
    @Bean
    @StepScope
    public ItemStreamReader<Map<String, String>> metadbFilePublisherReader() {
        return new MetaDbFilePublisherReader();
    }

    /**
     * metadbFilePublisherWriter
     * @return
     */
    @Bean
    @StepScope
    public ItemStreamWriter<Map<String, String>> metadbFilePublisherWriter() {
        return new MetaDbFilePublisherWriter();
    }

    /**
     * metadbFilePublisherListener
     * @return
     */
    @Bean
    public StepExecutionListener metadbFilePublisherListener() {
        return new MetaDbFilePublisherListener();
    }

    /**
     * asyncLimsRequestThreadPoolTaskExecutor
     * @return
     */
    @Bean(name = "asyncLimsRequestThreadPoolTaskExecutor")
    @StepScope
    public ThreadPoolTaskExecutor asyncLimsRequestThreadPoolTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(asyncThreadPoolSize);
        executor.setMaxPoolSize(asyncThreadPoolMax);
        executor.initialize();
        return executor;
    }

    /**
     * processorThreadPoolTaskExecutor
     * @return
     */
    @Bean(name = "processorThreadPoolTaskExecutor")
    @StepScope
    public ThreadPoolTaskExecutor processorThreadPoolTaskExecutor() {
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setCorePoolSize(processorThreadPoolSize);
        threadPoolTaskExecutor.setMaxPoolSize(processorThreadPoolMax);
        threadPoolTaskExecutor.initialize();
        return threadPoolTaskExecutor;
    }

    /**
     * asyncItemProcessor
     * @return
     */
    @Bean
    @StepScope
    public ItemProcessor<String, Future<Map<String, Object>>> asyncItemProcessor() {
        AsyncItemProcessor<String, Map<String, Object>> asyncItemProcessor = new AsyncItemProcessor();
        asyncItemProcessor.setTaskExecutor(processorThreadPoolTaskExecutor());
        asyncItemProcessor.setDelegate(limsRequestProcessor());
        return asyncItemProcessor;
    }

    /**
     * limsRequestProcessor
     * @return
     */
    @Bean
    @StepScope
    public LimsRequestProcessor limsRequestProcessor() {
        return new LimsRequestProcessor();
    }

    /**
     * asyncItemWriter
     * @return
     */
    @Bean
    @StepScope
    public ItemWriter<Future<Map<String, Object>>> asyncItemWriter() {
        AsyncItemWriter<Map<String, Object>> asyncItemWriter = new AsyncItemWriter();
        asyncItemWriter.setDelegate(limsRequestWriter());
        return asyncItemWriter;
    }

    /**
     * limsRequestWriter
     * @return
     */
    @Bean
    @StepScope
    public ItemStreamWriter<Map<String, Object>> limsRequestWriter() {
        return new LimsRequestWriter();
    }

    /**
     * limsRequestReader
     * @return
     */
    @Bean
    @StepScope
    public ItemStreamReader<String> limsRequestReader() {
        return new LimsRequestReader();
    }

    /**
     * limsRequestListener
     * @return
     */
    @Bean
    public StepExecutionListener limsRequestListener() {
        return new LimsRequestListener();
    }
}
