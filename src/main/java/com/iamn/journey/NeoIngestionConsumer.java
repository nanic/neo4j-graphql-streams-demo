package com.iamn.journey;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import javax.annotation.PostConstruct;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.google.gson.Gson;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

@Component
public class NeoIngestionConsumer {

    private static Logger logger = Logger.getLogger(NeoIngestionConsumer.class);

    @Autowired
    private StreamsConfig streamsConfig;
    
    @Value("${streams.kafka.topic}")
    private String kafkaTopic;

    @Value("${streams.neo4j.url}")
    private String neoUrl;
    
    private static Gson gson = new Gson();
    
    private String metadata, template = ""; 
    
    private static Map<String, Map<String, String>> meta = new HashMap<String, Map<String, String>>();
    private static Map<String, String> originalDataMap = new HashMap<String, String>();
    
    @SuppressWarnings("unchecked")
	@PostConstruct
    public void prepareMetadata() {
    	
    	InputStream in = this.getClass().getResourceAsStream("/flights-metadata.txt");
    	try {
			metadata = IOUtils.toString(in, Charset.defaultCharset());
			template = FileUtils.readFileToString(new File("src/main/resources/mutations.txt"), Charset.defaultCharset());
		} catch (IOException e) {
			logger.error("Failed reading metadata file with error, "+e.getMessage());
		}
    
    	meta = (Map<String, Map<String, String>>)gson.fromJson(metadata, meta.getClass());
    	
    }

	@SuppressWarnings("unchecked")
	@Bean
    public String ingestToNeo4j() {
    	
    	final StreamsBuilder builder = new StreamsBuilder();
    	KStream<Integer, String> source = builder.stream(kafkaTopic);
    	
        source.foreach((x, y) -> {
     
            originalDataMap = gson.fromJson(y, originalDataMap.getClass());
            
            Map<String, String> dataMap = new HashMap<String, String>();
        	
        	meta.forEach((k, v) -> {
        		v.forEach((k1, v1) -> {
        			dataMap.put(v1, originalDataMap.get(k1));
        		});
        	});
        	
        	StringSubstitutor sub = new StringSubstitutor(dataMap);
        	String resolvedString = sub.replace(template);
        	
        	try {
				logger.info(postMutation(resolvedString));
			} catch (UnirestException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            
        });
        
        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, streamsConfig);
        final CountDownLatch latch = new CountDownLatch(1);
        
        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });
 
        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
        
        return "";
   
    }

    @Bean
	public String postMutation(String mutationPayload) throws UnirestException {
		
		Map<String, String> headers = new HashMap<String, String>();
		headers.put("Accept", "application/json");
		headers.put("Content-Type", "application/json");
		headers.put("Authorization", "Basic bmVvNGo6bmVvNGpf");
		String response = Unirest.post(neoUrl).headers(headers).body(mutationPayload).asJson().getBody().toString();
		return response;
		
	}

}
