/**
 * 
 */
package org.apache.flink.gelly.embedded.neo4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Alberto De Lazzari
 *
 */
public class GellyBaseConfig {

	private static final Logger LOGGER = LoggerFactory.getLogger(GellyBaseConfig.class);

	public static Collection<Vertex<Long, Map<String, Object>>> vertices = new ArrayList<>();

	public static Collection<Edge<Long, Map<String, Object>>> edges = new ArrayList<>();

	public static final String LABEL = "DummyNode";

	public static final String TYPE = "DummyRelationship";

	@BeforeClass
	public static void initGraph() {
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put("description", "an item");
		properties.put("rate", Math.floor(Math.random() * 10));
		properties.put("percentage", Math.random());
		properties.put("labels", Collections.singletonList(LABEL));

		Vertex<Long, Map<String, Object>> v1 = new Vertex<>(1L, properties);
		Vertex<Long, Map<String, Object>> v2 = new Vertex<>(2L, properties);
		Vertex<Long, Map<String, Object>> v3 = new Vertex<>(3L, properties);

		vertices.add(v1);
		vertices.add(v2);
		vertices.add(v3);

		LOGGER.debug("Vertices: {}", vertices);

		// edge v1 --> v2
		properties = new HashMap<>();
		properties.put("distance", Math.random());
		properties.put("type", TYPE);

		Edge<Long, Map<String, Object>> e1 = new Edge<Long, Map<String, Object>>(v1.f0, v2.f0, properties);

		// edge v2 --> v3
		properties = new HashMap<>();
		properties.put("distance", Math.random());
		properties.put("type", TYPE);
		Edge<Long, Map<String, Object>> e2 = new Edge<Long, Map<String, Object>>(v2.f0, v3.f0, properties);

		edges.add(e1);
		edges.add(e2);

		LOGGER.debug("Edges: {}", edges);
	}

	@AfterClass
	public static void clearGraph() {
		vertices.clear();
		edges.clear();
	}
}
