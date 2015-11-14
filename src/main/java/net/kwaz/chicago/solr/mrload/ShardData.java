package net.kwaz.chicago.solr.mrload;

import com.google.common.base.Preconditions;

public class ShardData {
	
	private final String baseUrl;
	private final String coreName;
	
	public ShardData(String baseUrl, String coreName) {
		this.baseUrl = Preconditions.checkNotNull(baseUrl);
		this.coreName = Preconditions.checkNotNull(coreName);
	}
	
	public String getBaseUrl() {
		return baseUrl;
	}
	public String getCoreName() {
		return coreName;
	}

	@Override
	public String toString() {
		return "ShardData [baseUrl=" + baseUrl + ", coreName=" + coreName + "]";
	}
	
}
