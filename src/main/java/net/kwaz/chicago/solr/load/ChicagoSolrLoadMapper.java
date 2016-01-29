package net.kwaz.chicago.solr.load;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import net.kwaz.chicago.ChicagoKey;
import net.kwaz.chicago.ChicagoValue;
import net.kwaz.chicago.json.ChicagoJsonWriter;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

public class ChicagoSolrLoadMapper extends Mapper<ChicagoKey, ChicagoValue, NullWritable, NullWritable> {
	
	private static final String COUNTER_GROUP = "Counters";
	private static final String COUNTER_POSTS = "Posts";
	
	private static final int BATCH_SIZE = 1000;
	private int count = 0;
	
	private CloseableHttpClient client = null;
	private String postUrl = null;
	private ChicagoJsonWriter writer = null;
	private ByteArrayOutputStream bos = null;
	private JsonGenerator generator = null;
	private JsonFactory factory = null;
	
	@Override
	public void setup(Context context) throws IOException {
		postUrl = context.getConfiguration().get(ChicagoSolrLoad.POST_URL);
		client = HttpClients.createDefault();
		bos = new ByteArrayOutputStream();
		writer = new ChicagoJsonWriter();
		factory = new JsonFactory();
		generator = factory.createJsonGenerator(bos);
		generator.writeStartArray();
	}
	
	@Override
	public void map(ChicagoKey key, ChicagoValue value, Context context) throws IOException {
		writer.toJson(key, value, generator);
		
		if (++count > BATCH_SIZE) {
			count = 0;
			generator.writeEndArray();
			postToSolr(context);
			refreshStreams();
			generator.writeStartArray();
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException {
		if (count > 0) {
			generator.writeEndArray();
			postToSolr(context);
		}
		client.close();
		generator.close();
		bos.close();
	}
	
	private void refreshStreams() throws IOException {
		bos.close();
		bos = new ByteArrayOutputStream();
		generator.close();
		generator = factory.createJsonGenerator(bos);
	}

	private void postToSolr(Context context) throws IOException {
		generator.flush();
		bos.flush();
		HttpEntity entity = new ByteArrayEntity(bos.toByteArray());
		HttpPost post = new HttpPost(postUrl);
		post.addHeader("Content-type", "application/json");
		post.setEntity(entity);
		CloseableHttpResponse resp = client.execute(post);
		int responseCode = resp.getStatusLine().getStatusCode();
		if (responseCode != 200) {
			throw new IllegalStateException("Http " + responseCode + " returned");
		}
		HttpEntity respEntity = resp.getEntity();
		EntityUtils.consume(respEntity);
		resp.close();
		post.releaseConnection();
		
		context.getCounter(COUNTER_GROUP, COUNTER_POSTS).increment(1L);
	}
	
}
