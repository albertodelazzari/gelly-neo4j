/**
 * 
 */
package org.apache.flink.gelly.mapping.neo4j;

import java.util.Collection;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.mapping.neo4j.Neo4JSerializationMappingStrategy;
import org.apache.flink.mapping.neo4j.SerializationMapper;

/**
 * @author Alberto De Lazzari
 *
 */
public class GellyNeo4JSerializationMappingStrategy<V, E> extends
		Neo4JSerializationMappingStrategy<Tuple2<Collection<Vertex<Long, V>>, Collection<Edge<Long, E>>>, SerializationMapper<Tuple2<Collection<Vertex<Long, V>>, Collection<Edge<Long, E>>>>> {

	private static final long serialVersionUID = 1L;

	public GellyNeo4JSerializationMappingStrategy(String templateStatement, GellySerializationMapper<V, E> mapper) {
		super(templateStatement, mapper);
	}
}