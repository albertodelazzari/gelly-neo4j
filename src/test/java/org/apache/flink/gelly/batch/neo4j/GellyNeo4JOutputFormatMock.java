/**
 * 
 */
package org.apache.flink.gelly.batch.neo4j;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.gelly.embedded.neo4j.Neo4JBaseEmbeddedConfig;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.mapping.neo4j.DeserializationMapper;
import org.apache.flink.mapping.neo4j.Neo4JDeserializationMappingStrategy;

/**
 * @author Alberto De Lazzari
 *
 */
public class GellyNeo4JOutputFormatMock extends GellyNeo4JOutputFormat<Map<String, Object>, Map<String, Object>> {

	private static final long serialVersionUID = 1L;

	public GellyNeo4JOutputFormatMock(
			Neo4JDeserializationMappingStrategy<Tuple2<Collection<Vertex<Long, Map<String, Object>>>, Collection<Edge<Long, Map<String, Object>>>>, DeserializationMapper<Tuple2<Collection<Vertex<Long, Map<String, Object>>>, Collection<Edge<Long, Map<String, Object>>>>>> mappingStrategy,
			Map<String, String> config) {
		super(mappingStrategy, config);
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		super.open(taskNumber, numTasks);
		// We use a static session with an embedded Neo4J instance
		this.session = Neo4JBaseEmbeddedConfig.embeddedSession;
	}

}