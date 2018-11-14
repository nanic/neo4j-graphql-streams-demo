package com.iamn.journey;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.gson.Gson;
import com.mashape.unirest.http.exceptions.UnirestException;

/**
 * Unit test for simple StreamsApplication.
 */
public class NeoStreamsTest
{
	
	private static Logger logger = Logger.getLogger(NeoStreamsTest.class);
	
	private static Gson gson = new Gson();
	
	private static Map<String, String> originalDataMap = new HashMap<String, String>();
	
	private NeoIngestionConsumer consumer = new NeoIngestionConsumer();
	
	@Before
	public void setUp() {
		
	}
	
    @Test
    public void parseWeatherMessage() throws IOException {

        String weatherString = FileUtils.readFileToString(new File("src/test/resources/sample_weather.json"), Charsets.toCharset("UTF-8"));

        JSONObject obj = new JSONObject(weatherString);
        JSONObject obj1 = new JSONObject(obj.get("city").toString());
        String city = obj1.get("name").toString();

        System.out.println(city);
        assertEquals("Hurzuf", city);
    }
    
    @Test
	@Ignore
    public void parseFlightMessage() throws IOException, UnirestException {
    	
    	String metadata = FileUtils.readFileToString(new File("src/main/resources/flights-metadata.txt"), Charset.defaultCharset());
    	String data = FileUtils.readFileToString(new File("src/test/resources/sample-flight-msg.txt"), Charset.defaultCharset());
    	
    	originalDataMap = gson.fromJson(data, originalDataMap.getClass());
    	
    	Map<String, Map<String, String>> meta = new HashMap<String, Map<String, String>>();
    	meta = (Map<String, Map<String, String>>)gson.fromJson(metadata, meta.getClass());
    	
    	Map<String, String> dataMap = new HashMap<String, String>();
    	
    	meta.forEach((k, v) -> {
    		v.forEach((k1, v1) -> {
    			dataMap.put(v1, originalDataMap.get(k1));
    		});
    	});
    	
    	String template = FileUtils.readFileToString(new File("src/main/resources/mutations.txt"), Charset.defaultCharset());
    	
    	StringSubstitutor sub = new StringSubstitutor(dataMap);
    	String resolvedString = sub.replace(template);
    	
    	logger.info(resolvedString);
    	
    	consumer.postMutation(resolvedString);
    	
    }
    
}
