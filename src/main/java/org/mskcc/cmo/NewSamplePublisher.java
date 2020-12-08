package org.mskcc.cmo;

import com.google.gson.Gson;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.mskcc.cmo.messaging.Gateway;
import org.mskcc.cmo.shared.SampleMetadata;
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
     * Reads file and returns an instance of SampleMetadata
     *
     * @param fileName
     * @return
     */
    public SampleMetadata readFile(String fileName) {
        try {
            Gson gson = new Gson();
            Reader reader = Files.newBufferedReader(Paths.get(fileName));
            SampleMetadata s = gson.fromJson(reader, SampleMetadata.class);
            return s;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void run(String... args) throws Exception {
        String fileName = args[0];
        messagingGateway.connect();
        try {
            SampleMetadata s = readFile(fileName);
            System.out.println("Publishing new sample: " + s.getIgoId() + " on topic: " + topic);
            messagingGateway.publish(topic, s);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
