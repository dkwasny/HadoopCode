package net.kwaz.chicago.solr.mrload;

import java.util.Collections;
import java.util.concurrent.Callable;

import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest.MergeIndexes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class MyGoLiveWorker implements Callable<Integer> {
	
	private static final Logger LOG = LoggerFactory.getLogger(MyGoLiveWorker.class);
	
	private final ShardData shardData;
	private final Path indexPath;
	private final int id;
	
	public MyGoLiveWorker(ShardData shardData, Path indexPath, int id) {
		this.shardData = Preconditions.checkNotNull(shardData);
		this.indexPath = Preconditions.checkNotNull(indexPath);
		this.id = id;
	}

	@Override
	public Integer call() {
		HttpSolrServer solrServer = new HttpSolrServer(shardData.getBaseUrl());
		
		MergeIndexes mergeIndexesRequest = new MergeIndexes();
		mergeIndexesRequest.setCoreName(shardData.getCoreName());
		mergeIndexesRequest.setIndexDirs(
			Collections.singletonList(
				indexPath.toString()
			)
		);
		
		try {
			solrServer.request(mergeIndexesRequest);
		} catch (Exception e) {
			// Yea yea...this is garbage exception handling.
			// -Kwaz
			throw new RuntimeException(e);
		}
		
		LOG.info("Finished merging " + indexPath + " into " + shardData);
		
		return id;
	}
	
}
