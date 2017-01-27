/**
 * 
 */
package org.apache.flink.gelly.mapping.neo4j;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Alberto De Lazzari
 *
 */
public class GellySerializationDefaultMapper<V extends Map<String, Object>, E extends Map<String, Object>>
		implements GellySerializationMapper<V, E> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOGGER = LoggerFactory.getLogger(GellySerializationDefaultMapper.class);

	/**
	 * This is a default implementation of the serialize method where a Neo4J
	 * node is serialized as a set (Map) of properties and a Long number (node
	 * id) in order to have an object of type Vertex&lt;Long, Map&lt;String,
	 * Object&gt;&gt;
	 * 
	 * @param record
	 *            a record (as a Map) that will be returned by the cypher query
	 * @return a tuple that consists of two collections
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Tuple2<Collection<Vertex<Long, V>>, Collection<Edge<Long, E>>> serialize(Map<String, Object> record) {
		Tuple2<Collection<Vertex<Long, V>>, Collection<Edge<Long, E>>> tuple2 = new Tuple2<>();
		for (Entry<String, Object> entry : record.entrySet()) {
			Object value = entry.getValue();
			if (value instanceof Path) {
				Path path = (Path) value;
				Stream<Node> nodes = StreamSupport.stream(path.nodes().spliterator(), true);
				List<Vertex<Long, V>> vertices = nodes.map(n -> new Vertex<Long, V>(n.id(), (V) n.asMap()))
						.collect(Collectors.toList());

				LOGGER.debug("vertices: {}", vertices.toString());
				tuple2.f0 = vertices;

				Stream<Relationship> relationships = StreamSupport.stream(path.relationships().spliterator(), true);
				List<Edge<Long, E>> edges = relationships
						.map(r -> new Edge<Long, E>(r.startNodeId(), r.endNodeId(), (E) r.asMap()))
						.collect(Collectors.toList());

				LOGGER.debug("edges: {}", edges);
				tuple2.f1 = edges;
			}
		}

		return tuple2;
	}
}
