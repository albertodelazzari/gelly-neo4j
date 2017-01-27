package org.apache.flink.gelly.batch.neo4j;

import java.util.Map;

import org.apache.flink.gelly.embedded.neo4j.Neo4JBaseEmbeddedConfig;
import org.apache.flink.gelly.mapping.neo4j.GellyNeo4JSerializationDefaultMappingStrategy;

public class GellyNeo4JInputFormatMock extends GellyNeo4JInputFormat<Map<String, Object>, Map<String, Object>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public GellyNeo4JInputFormatMock(
			GellyNeo4JSerializationDefaultMappingStrategy<Map<String, Object>, Map<String, Object>> mappingStrategy,
			Map<String, String> config) {
		super(mappingStrategy, config);
	}

	@Override
	public void openInputFormat() {
		super.openInputFormat();
		// We use a static session with an embedded Neo4J instance
		session = Neo4JBaseEmbeddedConfig.embeddedSession;
	}
}
