/**
 * 
 */
package org.apache.flink.gelly.mapping.neo4j;

import java.util.Collection;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.mapping.neo4j.SerializationMapper;

/**
 * @author Alberto De Lazzari
 *
 */
@FunctionalInterface
public interface GellySerializationMapper<V, E>
		extends SerializationMapper<Tuple2<Collection<Vertex<Long, V>>, Collection<Edge<Long, E>>>> {

	/**
	 * This methods define a serialization from a record (as Map) that is
	 * returned by a cypher query and two collections: </br>
	 * <ul>
	 * <li>a collection of vertices</li>
	 * <li>a collection of edges</li>
	 * </ul>
	 * 
	 * @param record
	 *            a record (as a Map) that will be returned by the cypher query
	 * @return a tuple that consists of two collections
	 * 
	 * @see Tuple2
	 */
	@Override
	public Tuple2<Collection<Vertex<Long, V>>, Collection<Edge<Long, E>>> serialize(Map<String, Object> record);
}