/**
 * 
 */
package org.apache.flink.gelly.mapping.neo4j;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.gelly.embedded.neo4j.Neo4JBaseEmbeddedConfig;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.types.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Alberto De Lazzari
 *
 */
public class GellySerializationDefaultMapperTest extends Neo4JBaseEmbeddedConfig {

	private static final Logger LOGGER = LoggerFactory.getLogger(GellySerializationDefaultMapperTest.class);

	@Test
	public void testSerialize() {
		GellySerializationDefaultMapper<Map<String, Object>, Map<String, Object>> defaultMapper = new GellySerializationDefaultMapper<>();

		StatementResult result = Neo4JBaseEmbeddedConfig.embeddedSession.run("MATCH path = (v1:VERTEX)-[*]-(v2:VERTEX)-[*]-(v3:VERTEX) return path");
		Record record = result.list().get(0);

		Map<String, Object> item = record.asMap();
		Tuple2<Collection<Vertex<Long, Map<String, Object>>>, Collection<Edge<Long, Map<String, Object>>>> graph = defaultMapper
				.serialize(item);

		Collection<Vertex<Long, Map<String, Object>>> vertices = graph.f0;
		Collection<Edge<Long, Map<String, Object>>> edges = graph.f1;

		LOGGER.debug("Vertices should be not empty: {}", vertices);
		Assert.assertTrue(!vertices.isEmpty());

		Path path = (Path) item.get("path");
		path.nodes().forEach(n -> {
			List<Vertex<Long, Map<String, Object>>> sameVertices = vertices.stream().filter(v -> v.f0 == n.id())
					.collect(Collectors.toList());
			Assert.assertTrue(sameVertices.size() == 1);

			Vertex<Long, Map<String, Object>> vertex = sameVertices.get(0);
			Map<String, Object> vertexProps = vertex.f1;

			LOGGER.debug("vertex props: {}", vertexProps);
			LOGGER.debug("node props: {}", n.asMap());

			Assert.assertTrue(vertexProps.equals(n.asMap()));
		});

		LOGGER.debug("Edges should be not empty: {}", edges);
		Assert.assertTrue(!edges.isEmpty());

		path.relationships().forEach(r -> {
			List<Edge<Long, Map<String, Object>>> sameEdges = edges.stream().filter(e -> e.f2.equals(r.asMap()))
					.collect(Collectors.toList());

			Assert.assertTrue(sameEdges.size() == 1);

			Edge<Long, Map<String, Object>> edge = sameEdges.get(0);
			Long startVertex = edge.f0;
			Long endVertex = edge.f1;

			LOGGER.debug("start node and end node shoul be consistent {} {}", startVertex, endVertex);
			Assert.assertTrue(r.startNodeId() == startVertex);
			Assert.assertTrue(r.endNodeId() == endVertex);
		});
	}
}
