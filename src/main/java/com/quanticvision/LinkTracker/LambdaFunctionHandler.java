package com.quanticvision.LinkTracker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

public class LambdaFunctionHandler implements RequestStreamHandler {

	Boolean debug = System.getenv("debug").equalsIgnoreCase("yes");

	public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {

		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
		OutputStreamWriter writer = new OutputStreamWriter(outputStream, "UTF-8");
		JSONObject payload;
		String base64Encoded = "";
		String client = null;
		String channel = null;
		String outboxid = null;
		String linkId = null;
		String url = null;
		String queueUrl = null;

		try {

			payload = (JSONObject) (new JSONParser()).parse(reader);

			if (debug) {
				System.out.println(payload);
			}

			if (payload.get("pathParameters") != null) {

				JSONObject pathParameters = (JSONObject) payload.get("pathParameters");
				if (pathParameters.get("Base64EncodedString") != null) {
					base64Encoded = (String) pathParameters.get("Base64EncodedString");
					if (debug) {
						System.out.println("Base 64 Encoded String: " + base64Encoded);
					}

					byte[] decodedBytes = Base64.getDecoder().decode(base64Encoded);

					String decodedString = new String(decodedBytes);
					if (debug) {
						System.out.println("Decoded Full String: " + decodedString);
					}

					String[] parameters = decodedString.split("\\|");

					if (parameters.length < 4) {
						System.out.println("Valores menor a 4");
					} else {

						client = parameters[0];
						channel = parameters[1];
						outboxid = parameters[2];
						linkId = parameters[3];
						url = parameters[4];
						queueUrl = System.getenv("SQSBaseUrl") + "ebs-" + client + "-" + channel + "-dlr";

						if (debug) {
							System.out.println("Parameters in String: { client : " + client + " }, {channel: " + channel
									+ "} ," + "{ outboxid : " + outboxid + " }, " + "{ linkId : " + linkId + " },"
									+ " { url : " + url + " }");
						}

					}
				}

			} else {
				if (debug) {
					System.out.println("Parameter are null.");
				}
				url = "https://www.quantivision.com";
			}

			String sendQueueReturn = sendMessageToQueue(queueUrl, outboxid, linkId);

			writer.write(buildResult(url));
			writer.close();

		} catch (ParseException e) {
			e.printStackTrace();
		}

	}

	/**
	 * 
	 * @param queueUrl
	 * @param outboxid
	 * @return
	 */
	public String sendMessageToQueue(String queueUrl, String outboxid, String linkId) {
		AmazonSQSClient sqs = new AmazonSQSClient();
		LocalTime localTime = LocalTime.now();
		LocalDate localDate = LocalDate.now();

		Map<String, Object> response = new HashMap<String, Object>();
		Map<String, Object> header = new HashMap<String, Object>();

		Map<String, Object> msg = new HashMap<String, Object>();
		Map<String, Object> msgBody = new HashMap<String, Object>();

		msgBody.put("status", System.getenv("ViewedStatusId"));
		msgBody.put("description", "Clicked");
		msgBody.put("date", LocalDate.now().toString() + " " + LocalTime.now().toString());
		msgBody.put("linkId", linkId);

		msg.put("id", outboxid);
		msg.put("to", "click");
		msg.put("body", msgBody);

		String message = JSONObject.toJSONString(msg);

		if (debug) {
			System.out.println("Writing to Queue: " + queueUrl);
			System.out.println("Writing to Queue JSON: " + message.toString());
		}
		SendMessageRequest send_msg_request = new SendMessageRequest().withQueueUrl(queueUrl).withMessageBody(message);
		SendMessageResult messageResult = sqs.sendMessage(send_msg_request);
		String mr = messageResult.toString();
		if (debug) {
			System.out.println("Results: " + mr);
		}

		header.put("Content-Type", "image/png");
		response.put("isBase64Encoded", "true");
		response.put("statusCode", "200");
		response.put("headers", header);
		response.put("body","iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=");

		if (debug) {
			System.out.println("API Response : " + JSONObject.toJSONString(response));
		}

		return JSONObject.toJSONString(response);

	}

	/**
	 * 
	 * @param url
	 * @return
	 */
	private String buildResult(String url) {
		Map<String, Object> response = new HashMap<String, Object>();
		Map<String, Object> header = new HashMap<String, Object>();
		Map<String, Object> body = new HashMap<String, Object>();
		
		header.put("Cache-Control", "no-cache, no-store, must-revalidate");
		header.put("Provided-By", "Quantic Vision, S.A.");
		header.put("Location", url);
		response.put("isBase64Encoded", "false");
		response.put("statusCode", "301");
		response.put("headers", header);
		response.put("body", JSONObject.toJSONString(body));

		if (debug) {
			System.out.println("API Response : " + JSONObject.toJSONString(response));
		}

		return JSONObject.toJSONString(response);
	}

}
