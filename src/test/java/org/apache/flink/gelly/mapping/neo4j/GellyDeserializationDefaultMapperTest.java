/**
 * 
 */
package org.apache.flink.gelly.mapping.neo4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.gelly.embedded.neo4j.GellyBaseConfig;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Alberto De Lazzari
 *
 */
public class GellyDeserializationDefaultMapperTest extends GellyBaseConfig {

	private static final Logger LOGGER = LoggerFactory.getLogger(GellyDeserializationDefaultMapperTest.class);

	@SuppressWarnings("unchecked")
	@Test
	public void testDeserialize() {
		GellyDeserializationDefaultMapper<Map<String, Object>, Map<String, Object>> deserializationDefaultMapper = new GellyDeserializationDefaultMapper<>();

		Tuple2<Collection<Vertex<Long, Map<String, Object>>>, Collection<Edge<Long, Map<String, Object>>>> item = new Tuple2<Collection<Vertex<Long, Map<String, Object>>>, Collection<Edge<Long, Map<String, Object>>>>(
				vertices, edges);
		Map<String, Object> graphEntities = deserializationDefaultMapper.deserialize(item);
		Assert.assertTrue(!graphEntities.isEmpty());

		Object nodes = graphEntities.get("nodes");
		Assert.assertNotNull(nodes);
		LOGGER.debug("Nodes should be a collection of nodes");
		Assert.assertTrue(Collection.class.isAssignableFrom(nodes.getClass()));

		List<?> nodeEntities = ((List<?>) nodes);
		Assert.assertTrue(!nodeEntities.isEmpty());

		Map<String, Object> node = (Map<String, Object>) nodeEntities.get(0);
		Assert.assertNotNull(node);

		LOGGER.debug("A node should have all the labels that we have defined as 'labels' property");
		Assert.assertTrue(((Collection<String>) node.get("labels")).contains(LABEL));

		// Find all the vertices that have the id equals to this node id
		List<Vertex<Long, Map<String, Object>>> sameVertices = new ArrayList<>();
		vertices.forEach(v -> {
			if (v.f0 == node.get("id")) {
				sameVertices.add(v);
			}
		});
		Assert.assertTrue(sameVertices.size() == 1);

		LOGGER.debug("Vertex and node should have the same properties");
		Vertex<Long, Map<String, Object>> vertex = sameVertices.get(0);
		Map<String, Object> vertexProps = vertex.f1;

		LOGGER.debug("vertex props: {}", vertexProps);
		LOGGER.debug("node props: {}", node);

		Object relationships = graphEntities.get("relationships");
		Assert.assertNotNull(relationships);
		LOGGER.debug("Relationships should be a collection of relationships");
		Assert.assertTrue(Collection.class.isAssignableFrom(relationships.getClass()));

		List<?> relationshipEntities = ((List<?>) relationships);
		Assert.assertTrue(!relationshipEntities.isEmpty());

		Map<String, Object> relationship = (Map<String, Object>) relationshipEntities.get(0);
		Assert.assertNotNull(relationship);

		LOGGER.debug("A relationship should have a type that we have defined as 'type' property");
		Assert.assertTrue(relationship.get("type").equals(TYPE));

		List<Edge<Long, Map<String, Object>>> sameEdges = new ArrayList<>();
		edges.forEach(e -> {
			if (e.f2.get("distance") == relationship.get("distance")) {
				sameEdges.add(e);
			}
		});
		Assert.assertTrue(sameEdges.size() == 1);

		LOGGER.debug("Edge and node should have the same properties");
		Edge<Long, Map<String, Object>> edge = sameEdges.get(0);
		Map<String, Object> edgeProps = edge.f2;

		LOGGER.debug("edge props: {}", edgeProps);
		LOGGER.debug("relationship props: {}", relationship);
	}
}
