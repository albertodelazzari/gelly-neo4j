/**
 * 
 */
package org.apache.flink.gelly.mapping.neo4j;

import java.util.ArrayList;
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
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;
import org.neo4j.driver.v1.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Alberto De Lazzari
 *
 */
public class GellyNeo4JDeserializationDefaultMappingStrategyTest extends Neo4JBaseEmbeddedConfig {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(GellyNeo4JDeserializationDefaultMappingStrategyTest.class);
	
	@Test
	public void testMappingStrategy() {
		Tuple2<Collection<Vertex<Long, Map<String, Object>>>, Collection<Edge<Long, Map<String, Object>>>> item = new Tuple2<>();
		item.f0 = vertices;
		item.f1 = edges;

		GellyDeserializationDefaultMapper<Map<String, Object>, Map<String, Object>> mapper = new GellyDeserializationDefaultMapper<>();
		GellyNeo4JDeserializationDefaultMappingStrategy<Map<String, Object>, Map<String, Object>> defaultMappingStrategy = new GellyNeo4JDeserializationDefaultMappingStrategy<Map<String, Object>, Map<String, Object>>(
				item, mapper);

		Statement statement = defaultMappingStrategy.getStatement(item);
		StatementResult result = embeddedSession.run(statement);

		List<Record> records = result.list();
		records.forEach(record -> {

			List<Pair<String, Value>> entities = record.fields();
			entities.forEach(entity -> {
				LOGGER.debug("Entity: {}", entity.value());

				if (entity.value().asEntity() instanceof Node) {
					Node node = (Node) entity.value().asEntity();
					// There should be a one-to-one match among vertices and
					// nodes
					List<Vertex<Long, Map<String, Object>>> filteredVertex = new ArrayList<>();
					vertices.forEach(v -> {
						if (v.f0 == node.get("id").asLong()) {
							filteredVertex.add(v);
						}
					});
					Assert.assertTrue(filteredVertex.size() == 1);

					// The number of properties will be the same but some key
					// are different
					Assert.assertTrue(node.asMap().size() == filteredVertex.get(0).f1.size());
				}

				if (entity.value().asEntity() instanceof Relationship) {
					Relationship relationship = (Relationship) entity.value().asEntity();
					// There should be a one-to-one match among edges and
					// relationships
					List<Edge<Long, Map<String, Object>>> filteredEdge = new ArrayList<>();
					edges.forEach(e -> {
						if (Double.parseDouble(e.f2.get("distance").toString()) == relationship.get("distance")
								.asDouble()) {
							filteredEdge.add(e);
						}
					});
					Assert.assertTrue(filteredEdge.size() == 1);

					// The number of properties will be the same but some key
					// are different
					// Assert.assertTrue(relationship.asMap().size() ==
					// filteredEdge.get(0).f2.size());
				}
			});
		});
	}
}