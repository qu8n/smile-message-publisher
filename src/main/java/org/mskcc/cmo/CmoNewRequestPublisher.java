package org.mskcc.cmo;

import com.google.gson.Gson;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.mskcc.cmo.messaging.Gateway;
import org.mskcc.cmo.shared.SampleManifest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

/**
 *
 * @author DivyaMadala
 */
@SpringBootApplication(scanBasePackages = "org.mskcc.cmo.messaging")
public class CmoNewRequestPublisher implements CommandLineRunner {

    @Autowired
    private Gateway messagingGateway;

    @Value("${igo.new_request_topic}")
    private String IGO_NEW_REQUEST_TOPIC;

    @Value("${lims.base_url}")
    private String limsBaseUrl;

    @Value("${lims.username}")
    private String limsUsername;

    @Value("${lims.password}")
    private String limsPassword;

    @Value("${lims.request_samples_endpoint}")
    private String limsRequestSamplesEndpoint;

    @Value("${lims.sample_manifest_endpoint}")
    private String limsSampleManifestEndpoint;

    private Map<String, List<String>> limsrestRequestErrors = new HashMap<>();

    private final Log log = LogFactory.getLog(CmoNewRequestPublisher.class);

    public static void main(String[] args) throws Exception {
        SpringApplication.run(CmoNewRequestPublisher.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        String requestIds = args[0];
        messagingGateway.connect();
        Gson gson = new Gson();
        try {
            for (String r : requestIds.split(",")) {
                Map<String, Object> request = fetchRequestAndSamplesFromLims(r);
                String requestJson = gson.toJson(request);
                log.info("\nPublishing IGO new request to MetaDB:\n\n"
                        + requestJson + "\n\n on topic: " + IGO_NEW_REQUEST_TOPIC);
                messagingGateway.publish(IGO_NEW_REQUEST_TOPIC, requestJson);
            }
            // print summary report if any errors encountered
            if (!limsrestRequestErrors.isEmpty()) {
                System.out.println(generateFailedRequestSamplesSummary());
            }
        } catch (Exception e) {
            log.error("Error encountered during attempt to process request ids - exiting...");
            throw new RuntimeException(e);
        }
    }

    /**
     * Generates message for failed request samples manifest report.
     * @return
     */
    private String generateFailedRequestSamplesSummary() {
        StringBuilder builder = new StringBuilder("\nERROR SUMMARY REPORT BY REQUEST\n");
        for (String requestId : limsrestRequestErrors.keySet()) {
            List<String> requestSamples = limsrestRequestErrors.get(requestId);
            builder.append("\nRequest: ")
                    .append(requestId)
                    .append(", errors: ")
                    .append(requestSamples.size())
                    .append("\n\tSamples: ")
                    .append(StringUtils.join(requestSamples, ","))
                    .append("\n");
        }
        return builder.toString();
    }

    /**
     * Returns request metadata and the sample manifest list for all samples given a request id.
     * Note that the project id is simply the prefix of the request id. LIMS team
     * confirmed that this assumption is okay to make.
     * @param requestId
     * @return Map
     * @throws Exception
     */
    private Map<String, Object> fetchRequestAndSamplesFromLims(String requestId) throws Exception {
        // call lims api for given request id...
        // LIMS endpoint: /publishIgoRequestToMetaDb,
        //      params: requestId (String), projectId (String, optional)
        // response = response from LIMS /publishIgoRequestToMetaDb endpoint
        // key-value pairs:
        // requestId: string
        // projectId: string
        // sampleManifestList: List<SampleManifest>

        // fetch request samples from getRequestSamples endpoint
        // then fetch sample manifest for each sample from getSampleManifest endpoint
        Map<String, Object> limsResponse = getLimsRequestSamples(requestId);
        List<SampleManifest> sampleManifestList = getSampleManifestList(requestId, limsResponse);
        String projectId = requestId.split("_")[0];
        limsResponse.put("projectId", projectId);
        limsResponse.put("sampleManifestList", sampleManifestList);
        return limsResponse;
    }

    /**
     * Calls LimsRest and returns list of SampleManifest instances for a request id.
     * @param response
     * @return List
     * @throws Exception
     */
    private List<SampleManifest> getSampleManifestList(String requestId,Map<String, Object> response)
            throws Exception {
        // get sample ids and compile into param to pass to getSampleManifest endpoint
        List<String> sampleIds = getSampleIdsFromRequestResponse(response);
        List<SampleManifest> sampleManifestList = new ArrayList<>();
        for (String sampleId : sampleIds) {
            List<SampleManifest> manifest = getSampleManifest(sampleId);
            if (manifest != null) {
                sampleManifestList.addAll(manifest);
            } else {
                // update map of requests with samples that we could not fetch the manifest for
                List<String> sList = limsrestRequestErrors.getOrDefault(requestId, new ArrayList<>());
                sList.add(sampleId);
                limsrestRequestErrors.put(requestId, sList);
            }
        }
        return sampleManifestList;
    }

    /**
     * Returns a list with a single sample manifest object given a sample id.
     * @param sampleId
     * @return List
     */
    private List<SampleManifest> getSampleManifest(String sampleId) throws Exception {
        String manifestUrl = limsBaseUrl + limsSampleManifestEndpoint + sampleId;
        log.debug("Sending request for sample manifest with url:" + manifestUrl);

        RestTemplate restTemplate = getRestTemplate();
        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = getRequestEntity();
        try {
            ResponseEntity<SampleManifest[]> responseEntity = restTemplate.exchange(manifestUrl,
                HttpMethod.GET, requestEntity, SampleManifest[].class);
            return Arrays.asList(responseEntity.getBody());
        } catch (HttpServerErrorException e) {
            if (e.getStatusCode().equals(HttpStatus.INTERNAL_SERVER_ERROR)) {
                log.error("Error encountered during attempt to fetch sample manifest for '"
                        + sampleId + "', request url: '" + manifestUrl + "'", e);
            }
        }
        return null;
    }

    /**
     * Calls LimsRest and returns the list of samples for a given request id.
     * @param requestId
     * @return Map
     * @throws Exception
     */
    private Map<String, Object> getLimsRequestSamples(String requestId) throws Exception {
        Gson gson = new Gson();
        String requestUrl = limsBaseUrl + limsRequestSamplesEndpoint + requestId;
        RestTemplate restTemplate = getRestTemplate();
        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = getRequestEntity();
        ResponseEntity responseEntity = restTemplate.exchange(requestUrl,
                HttpMethod.GET, requestEntity, Object.class);
        Map<String, Object> response = gson.fromJson(gson.toJson(responseEntity.getBody()), Map.class);
        log.debug("Response from LIMS:\n" + gson.toJson(response));
        return response;
    }

    /**
     * Extracts list of sample ids as strings from the LIMS response.
     * @param response
     * @return
     */
    private List<String> getSampleIdsFromRequestResponse(Map<String, Object> response) {
        Gson gson = new Gson();
        String samplesListJson = gson.toJson(response.get("samples"));
        List<Map> samplesListMap = gson.fromJson(samplesListJson, List.class);
        List<String> sampleIds = new ArrayList<>();
        for (Map m : samplesListMap) {
            sampleIds.add((String) m.get("igoSampleId"));
        }
        return sampleIds;
    }

    /**
     * Returns rest template that by passes SSL cert check.
     * @return RestTemplate
     * @throws Exception
     */
    private RestTemplate getRestTemplate() throws Exception {
        TrustStrategy acceptingTrustStrategy = (X509Certificate[] chain, String authType) -> true;
        HostnameVerifier hostnameVerifier = (s, sslSession) -> true;
        SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(null, acceptingTrustStrategy).build();
        SSLConnectionSocketFactory csf = new SSLConnectionSocketFactory(sslContext, hostnameVerifier);
        CloseableHttpClient httpClient = HttpClients.custom().setSSLSocketFactory(csf).build();
        HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory();
        requestFactory.setHttpClient(httpClient);
        return new RestTemplate(requestFactory);
    }

    /**
     * Returns request entity.
     * @return HttpEntity
     */
    private HttpEntity getRequestEntity() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBasicAuth(limsUsername, limsPassword);
        return new HttpEntity<Object>(headers);
    }
}
