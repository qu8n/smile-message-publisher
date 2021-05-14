package org.mskcc.cmo.publisher.pipeline.limsrest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;



/**
 *
 * @author ochoaa
 */
public class LimsRequestProcessor implements ItemProcessor<String, Map<String, Object>> {
    private static final Log LOG = LogFactory.getLog(LimsRequestProcessor.class);

    @Autowired
    private LimsRequestUtil limsRestUtil;

    @Override
    public Map<String, Object> process(String requestId) throws Exception {
        CompletableFuture<Map<String, Object>> futureRequestResponse =
                limsRestUtil.getLimsRequestSamples(requestId);
        Map<String, Object> requestResponse = futureRequestResponse.get();
        List<String> sampleIds = limsRestUtil.getSampleIdsFromRequestResponse(requestResponse);

        if (!requestResponse.containsKey("samples") || sampleIds == null || sampleIds.isEmpty()) {
            LOG.error("Parsing request with no samples" + requestId);
            return null;
        }

        // get sample manifest for each sample id
        List<Object> sampleManifestList = new ArrayList<>();
        for (String sampleId : sampleIds) {
            CompletableFuture<List<Object>> manifest = limsRestUtil.getSampleManifest(sampleId);
            if (manifest != null) {
                sampleManifestList.addAll(manifest.get());
            } else {
                limsRestUtil.updateLimsRequestErrors(requestId, sampleId);
            }
        }

        // update request response with sample manifests fetched
        // and add project id as well for cmo metadb
        String projectId = requestId.split("_")[0];
        // TODO: remove this key for now manually
        //later we wont be getting this anymore
        if (requestResponse.containsKey("isCmoRequest")) {
            requestResponse.remove("isCmoRequest");
        }
        requestResponse.put("projectId", projectId);
        requestResponse.put("samples", sampleManifestList);
        return requestResponse;
    }

}
