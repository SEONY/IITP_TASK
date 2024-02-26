package service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;

public class Rest {
	final static String USER_AGENT = "gop/1.0";

	public void sendGET(String requestURL) throws IOException {
		CloseableHttpClient httpClient = HttpClients.createDefault();
		HttpGet httpGet = new HttpGet(requestURL);
		httpGet.addHeader("User-Agent", USER_AGENT);
		CloseableHttpResponse httpResponse = httpClient.execute(httpGet);

		System.out.println("GET Response Status:: "
				+ httpResponse.getStatusLine().getStatusCode());

		BufferedReader reader = new BufferedReader(new InputStreamReader(
				httpResponse.getEntity().getContent()));

		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = reader.readLine()) != null) {
			response.append(inputLine);
		}
		reader.close();

		// print result
		System.out.println(response.toString());
		httpClient.close();
	}

	public void sendPOST(String requestURL, String jsonMessage) {

		CloseableHttpClient httpClient = HttpClients.createDefault();
		HttpPost httpPost = new HttpPost(requestURL);
		httpPost.addHeader("User-Agent", USER_AGENT);

		StringEntity requestEntity = new StringEntity(jsonMessage.toString(), "utf-8");
		requestEntity.setContentType(new BasicHeader("Content-Type", "text/plain"));
		httpPost.setEntity(requestEntity);

		// List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
		// urlParameters.add(new BasicNameValuePair("userName", "Pankaj Kumar"));

		// HttpEntity postParams = new UrlEncodedFormEntity(urlParameters);
		// httpPost.setEntity(postParams);

		CloseableHttpResponse httpResponse;
		try {
			httpResponse = httpClient.execute(httpPost);

			 System.out.println("POST Response Status:: " +
			 httpResponse.getStatusLine().getStatusCode());

			BufferedReader reader = new BufferedReader(new InputStreamReader(
					httpResponse.getEntity().getContent()));

			String inputLine;
			StringBuffer response = new StringBuffer();

			while ((inputLine = reader.readLine()) != null) {
				response.append(inputLine);
			}
			reader.close();

			// print result
			// System.out.println(response.toString());
			httpClient.close();
		} catch (Exception e) {
			//System.out.println("post err : "+httpResponse.getStatusLine().getStatusCode() + requestURL);
			System.out.println("post err : "+ requestURL);
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}
	}
}
