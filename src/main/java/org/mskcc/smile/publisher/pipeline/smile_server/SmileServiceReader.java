package org.mskcc.smile.publisher.pipeline.smile_server;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

/**
 *
 * @author ochoaa
 */
public class SmileServiceReader implements ItemStreamReader<String> {
    @Value("#{jobParameters[requestIds]}")
    private String requestIds;

    @Value("#{jobParameters[cmoRequestsFilter]}")
    private Boolean cmoRequestsFilter;

    @Value("${smile.request_id.chunk_size:25}")
    private int chunkSize;

    @Autowired
    private SmileServiceUtil smileServiceUtil;

    private List<String> smileRequestsList;

    private final ObjectMapper mapper = new ObjectMapper();
    private static final Log LOG = LogFactory.getLog(SmileServiceReader.class);

    @Override
    public void open(ExecutionContext ec) throws ItemStreamException {
        List<String> requestJSONs = new ArrayList<String>();
        List<String> rIds = Arrays.asList(requestIds.split(","));

        for (int i = 0; i < rIds.size(); i += chunkSize) {
            List<String> rIdChunk = rIds.subList(i, Math.min(i + chunkSize, rIds.size()));
            try {
                String requestJson = smileServiceUtil.getRequestsById(rIdChunk);
                mapper.readTree(requestJson).forEach(node -> requestJSONs.add(node.toString()));
            } catch (Exception ex) {
                LOG.error("Exception thrown while processing a request contained in the following chunk, "
                          + "dropping to query by single request id: " + rIdChunk);
                for (int j = 0; j < rIdChunk.size(); j++) {
                    List<String> rIdsChunkSub = rIdChunk.subList(j, Math.min(j + 1, rIdChunk.size()));
                    try {
                        String requestJson = smileServiceUtil.getRequestsById(rIdsChunkSub);
                        mapper.readTree(requestJson).forEach(node -> requestJSONs.add(node.toString()));
                    } catch (Exception e) {
                        LOG.error("Exception thrown while processing request, skipping: " + rIdsChunkSub);
                        LOG.error("Exception: ", e);
                    }
                }
            }
        }

        this.smileRequestsList = (cmoRequestsFilter) ? filterNonCMORequests(requestJSONs) : requestJSONs;
        LOG.info("Total requests publishing to topic: " + String.valueOf(smileRequestsList.size()));
    }

    private List<String> filterNonCMORequests(List<String> requestJSONs) {
        List<String> toReturn = new ArrayList<String>();
        for (String requestJSON : requestJSONs) {
            try {
                Map<String, Object> requestMap = mapper.readValue(requestJSON, Map.class);
                Boolean isCmoRequest = (Boolean) requestMap.getOrDefault("isCmoRequest", Boolean.FALSE);
                if (isCmoRequest) {
                    toReturn.add(requestJSON);
                    continue;
                }
                String requestId = (String) requestMap.get("igoRequestId");
                LOG.info("Skipping non-CMO request: " + requestId + "...");
            } catch (Exception e) {
                LOG.error("Exception thrown while filtering request, "
                          + "not including request in published set...");
                LOG.error("Exception: ", e);
            }
        }
        return toReturn;
    }

    @Override
    public void update(ExecutionContext ec) throws ItemStreamException {}

    @Override
    public void close() throws ItemStreamException {}

    @Override
    public String read() throws Exception {
        if (!smileRequestsList.isEmpty()) {
            return smileRequestsList.remove(0);
        }
        return null;
    }

}
