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
    private static int numOfSamples;

    private static String fileName;

    private static List<SampleMetadata> meta;

    @Value("${igo_new_sample}")
    private static String topic;

    public static void main(String[] args) throws Exception {
        SpringApplication.run(NewSamplePublisher.class, args);
    }


    /**
     * Creates mock sample data.. when neo4j is hooked up we can uncomment the "addSample" calls.
     * @param sampleId
     * @return
     */
    private static SampleMetadata mockSampleMetadata(String sampleId) {
        SampleMetadata sMetadata = new SampleMetadata();
        sMetadata.setInvestigatorSampleId(sampleId);
        sMetadata.setIgoId("IGO-" + sampleId);
        sMetadata.setSampleOrigin("");
        sMetadata.setSex("");
        sMetadata.setSpecies("");
        sMetadata.setSpecimenType("");
        sMetadata.setTissueLocation("");
        sMetadata.setTumorOrNormal("TUMOR");
        // sMetadata.addSample(new Sample("P-0002978-IM5-T02", "DMP"));
        // sMetadata.addSample(new Sample("DrilA_NTRK_X_0001_JV_P1", "DARWIN"));
        // sMetadata.addSample(new Sample("s_C_000520_X001_d", "CMO"));
        return sMetadata;
    }

    /**
     * Unused for now.
     * @param patientId
     * @return
     */
    private PatientMetadata mockPatientMetadata(String patientId) {
        PatientMetadata pMetadata = new PatientMetadata();
        pMetadata.setInvestigatorPatientId(patientId);
        pMetadata.addPatient(new Patient("P-0002978", "DMP"));
        pMetadata.addPatient(new Patient("215727", "DARWIN"));
        return pMetadata;
    }

    @Override
    public void run(String... args) throws Exception {
        Gson gson = new Gson();
        messagingGateway.connect();
        try {
            List<String> sampleIds = Arrays.asList(new String[]{"sample1", "sample2", "sample3", "sample4"});
            for (String sampleId : sampleIds) {
                System.out.println("Publishing new sample: " + sampleId);
                SampleMetadata s = mockSampleMetadata(sampleId);
                messagingGateway.publish(topic, s);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
