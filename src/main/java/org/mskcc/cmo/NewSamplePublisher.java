package org.mskcc.cmo;

import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.mskcc.cmo.messaging.Gateway;
import org.mskcc.cmo.shared.SampleMetadata;
import org.mskcc.cmo.shared.neo4j.Patient;
import org.mskcc.cmo.shared.neo4j.PatientMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 *
 * @author DivyaMadala
 */
@SpringBootApplication(scanBasePackages = "org.mskcc.cmo.messaging")
public class NewSamplePublisher implements CommandLineRunner {

    @Autowired
    private Gateway messagingGateway;

    @Value("${igo.new_sample_intake_topic}")
    private String topic;

    public static void main(String[] args) throws Exception {
        SpringApplication.run(NewSamplePublisher.class, args);
    }


    /**
     * Creates mock sample data.. when neo4j is hooked up we can uncomment the "addSample" calls.
     * @param sampleId
     * @return
     */
    private SampleMetadata mockSampleMetadata(String sampleId) {
        SampleMetadata sMetadata = new SampleMetadata();
        sMetadata.setInvestigatorSampleId(sampleId);
        sMetadata.setIgoId("IGO-" + sampleId);
        sMetadata.setSampleOrigin("");
        sMetadata.setSex("");
        sMetadata.setSpecies("");
        sMetadata.setSpecimenType("");
        sMetadata.setTissueLocation("");
        sMetadata.setTumorOrNormal("TUMOR");
        return sMetadata;
    }

    @Override
    public void run(String... args) throws Exception {
        messagingGateway.connect();
        try {
            List<String> sampleIds = Arrays.asList(new String[]{"sample1", "sample2", "sample3", "sample4"});
            for (String sampleId : sampleIds) {
                System.out.println("Publishing new sample: " + sampleId + " on topic: " + topic);
                SampleMetadata s = mockSampleMetadata(sampleId);
                messagingGateway.publish(topic, s);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
