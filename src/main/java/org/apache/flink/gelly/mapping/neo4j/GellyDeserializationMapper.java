/**
 * 
 */
package org.apache.flink.gelly.mapping.neo4j;

import java.util.Collection;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.mapping.neo4j.DeserializationMapper;

/**
 * @author Alberto De Lazzari
 *
 */
@FunctionalInterface
public interface GellyDeserializationMapper<V, E>
		extends DeserializationMapper<Tuple2<Collection<Vertex<Long, V>>, Collection<Edge<Long, E>>>> {

	/**
	 * Convert a Gelly graph (represented by a collection of vertices and edges)
	 * into a key-value pairs (used by a cypher statement)
	 * 
	 * @param
	 * @return a key-value set that will be used in a cypher statement
	 */
	@Override
	public Map<String, Object> deserialize(Tuple2<Collection<Vertex<Long, V>>, Collection<Edge<Long, E>>> item);
}
