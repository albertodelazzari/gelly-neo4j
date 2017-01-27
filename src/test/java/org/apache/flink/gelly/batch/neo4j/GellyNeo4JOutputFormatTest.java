/**
 * 
 */
package org.apache.flink.gelly.batch.neo4j;

import java.util.Collection;
import java.util.Map;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.gelly.embedded.neo4j.Neo4JBaseEmbeddedConfig;
import org.apache.flink.gelly.mapping.neo4j.GellyDeserializationDefaultMapper;
import org.apache.flink.gelly.mapping.neo4j.GellyNeo4JDeserializationDefaultMappingStrategy;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Alberto De Lazzari
 *
 */
public class GellyNeo4JOutputFormatTest extends Neo4JBaseEmbeddedConfig {

	private static final Logger LOGGER = LoggerFactory.getLogger(GellyNeo4JOutputFormatTest.class);

	@Test
	public void testOutputFormat() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		// Create input
		Tuple2<Collection<Vertex<Long, Map<String, Object>>>, Collection<Edge<Long, Map<String, Object>>>> item = new Tuple2<>();
		item.f0 = vertices;
		item.f1 = edges;

		GellyDeserializationDefaultMapper<Map<String, Object>, Map<String, Object>> mapper = new GellyDeserializationDefaultMapper<>();
		GellyNeo4JDeserializationDefaultMappingStrategy<Map<String, Object>, Map<String, Object>> defaultMappingStrategy = new GellyNeo4JDeserializationDefaultMappingStrategy<Map<String, Object>, Map<String, Object>>(
				item, mapper);

		DataSet<Tuple2<Collection<Vertex<Long, Map<String, Object>>>, Collection<Edge<Long, Map<String, Object>>>>> dataSet = env
				.fromElements(item);

		dataSet.output(new GellyNeo4JOutputFormatMock(defaultMappingStrategy, neo4JConfig));

		env.execute();

		LOGGER.debug("count: {}", dataSet.count());
	}
}
