package org.mskcc.smile.publisher.pipeline.smile_server;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.RestTemplate;

/**
 *
 * @author ochoaa
 */
@Component
public class SmileServiceUtil {
    private final ObjectMapper mapper = new ObjectMapper();
    @Value("${smile.base_url}")
    private String smileBaseUrl;
    @Value("${smile.request_endpoint}")
    private String smileRequestEndpoint;

    /**
     * Given a requestID, returns response from the SMILE web service.
     * @param requestIds
     * @return String
     * @throws Exception
     */
    public String getRequestsById(List<String> requestIds) throws Exception {
        String requestUrl = smileBaseUrl + smileRequestEndpoint;
        RestTemplate restTemplate = new RestTemplate();
        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = getRequestEntity(requestIds);
        ResponseEntity responseEntity = restTemplate.exchange(requestUrl,
                HttpMethod.POST, requestEntity, Object.class);
        String requestJsonString = mapper.writeValueAsString(responseEntity.getBody());
        return requestJsonString;
    }

    /**
     * Returns request entity.
     * @param requestIds
     * @return HttpEntity
     */
    private HttpEntity getRequestEntity(List<String> requestIds) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return new HttpEntity<Object>(requestIds, headers);
    }
}
