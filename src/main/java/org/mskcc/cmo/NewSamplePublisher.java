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
import org.mskcc.cmo.messaging.impl.NATSGatewayImpl;
import org.mskcc.cmo.shared.SampleMetadata;
import org.mskcc.cmo.shared.neo4j.Patient;
import org.mskcc.cmo.shared.neo4j.PatientMetadata;
import org.mskcc.cmo.shared.neo4j.Sample;
import org.mskcc.cmo.shared.neo4j.SampleMetadataEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 *
 * @author DivyaMadala
 */
@SpringBootApplication(scanBasePackages = "org.mskcc.cmo.messaging")
public class NewSamplePublisher implements CommandLineRunner {
    private final String NEW_SAMPLE_INTAKE = "igo.new-sample-intake";
    @Value("${nats.clusterid}")
    private String natsClusterId;

    @Autowired
    private Gateway messagingGateway;
    private static int numOfSamples;

    private static String fileName;

    private static List<SampleMetadata> meta;

    @Value("${igo_new_sample}")
    private static String topic;

    private static void parseArgs(String[] args) {
        List<String> argList = new ArrayList<String>(Arrays.asList(args));
        Iterator<String> argIterator = argList.iterator();

        while (argIterator.hasNext()) {
            String next = argIterator.next();
            switch (next) {
                case "-n":
                    numOfSamples = Integer.parseInt(argIterator.next());
                    argIterator.remove();
                    continue;
                case "-f":
                    fileName = argIterator.next();
                    argIterator.remove();
                    continue;
                default:
                    continue;
            }
        }
    }

    private static void getData() {
        try {
            Reader reader = Files.newBufferedReader(Paths.get(fileName));
            meta = new Gson().fromJson(reader, new TypeToken<List<SampleMetadata>>() {
            }.getType());
            // add check for number of samples?
            reader.close();
        } catch (JsonIOException | JsonSyntaxException | IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        SpringApplication.run(NewSamplePublisher.class, args);
    }


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
//        sMetadata.addSample(new Sample("P-0002978-IM5-T02", "DMP"));
//        sMetadata.addSample(new Sample("DrilA_NTRK_X_0001_JV_P1", "DARWIN"));
//        sMetadata.addSample(new Sample("s_C_000520_X001_d", "CMO"));
        return sMetadata;
    }

//    private PatientMetadata mockPatientMetadata(String patientId) {
//        PatientMetadata pMetadata = new PatientMetadata();
//        pMetadata.setInvestigatorPatientId(patientId);
//        pMetadata.addPatient(new Patient("P-0002978", "DMP"));
//        pMetadata.addPatient(new Patient("215727", "DARWIN"));
//        return pMetadata;
//    }

    @Override
    public void run(String... args) throws Exception {
        Gson gson = new Gson();
        System.out.println("\n\nNATS CLUSER ID");
        System.out.println(natsClusterId);
        messagingGateway.connect();
        try {
            List<String> sampleIds = Arrays.asList(new String[]{"sample1", "sample2", "sample3", "sample4"});
            for (String sampleId : sampleIds) {
                System.out.println("Publishing new sample: " + sampleId);
                SampleMetadata s = mockSampleMetadata(sampleId);
                messagingGateway.publish(NEW_SAMPLE_INTAKE, s);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
