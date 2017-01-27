package org.apache.flink.gelly.mapping.neo4j;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.gelly.embedded.neo4j.Neo4JBaseEmbeddedConfig;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GellyNeo4JSerializationDefaultMappingStategyTest extends Neo4JBaseEmbeddedConfig {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(GellyNeo4JSerializationDefaultMappingStategyTest.class);

	@Test
	public void testMappingStrategy() {

		// Define the default mapper
		GellySerializationDefaultMapper<Map<String, Object>, Map<String, Object>> defaultMapper = new GellySerializationDefaultMapper<>();

		// We match a simple path v1 --> v2
		String statement = "MATCH path = (v1:VERTEX)-[e:EDGE]->(v2:VERTEX) return path";

		// Create the serialization strategy from Neo4J to Flink
		GellyNeo4JSerializationDefaultMappingStrategy<Map<String, Object>, Map<String, Object>> defaultMappingStrategy = new GellyNeo4JSerializationDefaultMappingStrategy<Map<String, Object>, Map<String, Object>>(
				statement, defaultMapper);

		StatementResult result = embeddedSession.run(statement);
		List<Record> records = result.list();

		int numOfNodes = 0, numOfRelationships = 0;
		for (Record record : records) {
			LOGGER.debug("record: {}", record);
			Tuple2<Collection<Vertex<Long, Map<String, Object>>>, Collection<Edge<Long, Map<String, Object>>>> item = defaultMappingStrategy
					.map(record);

			Collection<Vertex<Long, Map<String, Object>>> vertices = item.f0;
			LOGGER.debug("nodes: {}", vertices);

			numOfNodes += vertices.size();

			Collection<Edge<Long, Map<String, Object>>> edges = item.f1;
			LOGGER.debug("relationships: {}", edges);

			numOfRelationships += edges.size();
		}

		// Our embedded Neo4J database has 3 nodes but we extract 2 pattern so
		// one node will be counted twice
		Assert.assertTrue(numOfNodes == 4);

		// Our embedded Neo4J database has 2 relationships
		Assert.assertTrue(numOfRelationships == 2);
	}
}