package org.mskcc.cmo.publisher.pipeline.limsrest;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.mskcc.cmo.messaging.Gateway;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

/**
 *
 * @author ochoaa
 */
public class LimsRequestWriter implements ItemStreamWriter<Map<String, Object>> {

    @Autowired
    private Gateway messagingGateway;

    @Value("${lims.publisher_topic}")
    private String LIMS_PUBLISHER_TOPIC;

    private ObjectMapper mapper = new ObjectMapper();
    private final Logger LOG = Logger.getLogger(LimsRequestWriter.class);

    @Override
    public void open(ExecutionContext ec) throws ItemStreamException {}

    @Override
    public void update(ExecutionContext ec) throws ItemStreamException {}

    @Override
    public void close() throws ItemStreamException {}

    @Override
    public void write(List<? extends Map<String, Object>> requestResponseList) throws Exception {
        for (Map<String, Object> request : requestResponseList) {
            try {
                String requestJson = mapper.writeValueAsString(request);
                LOG.debug("\nPublishing IGO new request to MetaDB:\n\n"
                        + requestJson + "\n\n on topic: " + LIMS_PUBLISHER_TOPIC);
                messagingGateway.publish(LIMS_PUBLISHER_TOPIC, requestJson);
            } catch (Exception e) {
                LOG.error("Error encountered during attempt to process request ids - exiting...");
                throw new RuntimeException(e);
            }
        }
    }

}
