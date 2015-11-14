package net.kwaz.chicago.solr.mrload;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class MyGoLive {
	
	private static final Logger LOG = LoggerFactory.getLogger(MyGoLive.class);
	
	private final String collectionName;
	private final CloudSolrServer cloudServer;
	
	public MyGoLive(String zkQuorum, String collectionName)
	{
		this(
			collectionName,
			new CloudSolrServer(zkQuorum)
		);
	}
	
	public MyGoLive(String collectionName, CloudSolrServer cloudServer)
	{
		this.collectionName = Preconditions.checkNotNull(collectionName);
		this.cloudServer = Preconditions.checkNotNull(cloudServer);
	}
	
	public void mergeIndexes(String collectionName, Collection<ShardData> shardDatas, Collection<Path> indexPaths)
	throws SolrServerException, IOException, InterruptedException, ExecutionException
	{
		Preconditions.checkNotNull(shardDatas);
		Preconditions.checkNotNull(indexPaths);
		Preconditions.checkArgument(shardDatas.size() == indexPaths.size());
		
		Iterator<ShardData> shardDataIterator = shardDatas.iterator();
		Iterator<Path> indexPathsIterator = indexPaths.iterator();
		
		final int numThreads = shardDatas.size();
		
		ExecutorService executor = Executors.newFixedThreadPool(numThreads);
		CompletionService<Integer> completion = new ExecutorCompletionService<Integer>(executor);
		
		int id = 0;
		while (shardDataIterator.hasNext() && indexPathsIterator.hasNext()) {
			ShardData shardData = shardDataIterator.next();
			Path indexPath = indexPathsIterator.next();
			
			LOG.info("Starting merge of " + indexPath + " into " + shardData);
			completion.submit(
				new MyGoLiveWorker(shardData, indexPath, ++id)
			);
		}
		
		int numCompleted = 0;
		while (numCompleted != numThreads) {
			Future<Integer> future = completion.take();
			LOG.info("Thread " + future.get() + " completed");
			++numCompleted;
		}
		executor.shutdown();
		
		LOG.info("Committing merge");
		cloudServer.setDefaultCollection(collectionName);
		cloudServer.commit();
	}

	public Collection<ShardData> getShardDatas()
	{
		cloudServer.connect();
		ZkStateReader zkStateReader = cloudServer.getZkStateReader();
		
		ClusterState clusterState = zkStateReader.getClusterState();
		DocCollection docCollection = clusterState.getCollection(collectionName);

		Collection<Slice> slices = docCollection.getSlices();
		List<ZkCoreNodeProps> coreNodeProps = Lists.newArrayList();
		for (Slice slice : slices) {
			Replica replica = slice.getLeader();
			coreNodeProps.add(new ZkCoreNodeProps(replica));
		}
		
		List<ShardData> retVal = Lists.newArrayList();
		for (ZkCoreNodeProps coreNodeProp : coreNodeProps) {
			retVal.add(
				new ShardData(
					coreNodeProp.getBaseUrl(),
					coreNodeProp.getCoreName()
				)
			);
		}
		
		return retVal;
	}

}
