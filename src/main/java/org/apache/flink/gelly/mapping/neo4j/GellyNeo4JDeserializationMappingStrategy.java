/**
 * 
 */
package org.apache.flink.gelly.mapping.neo4j;

import java.util.Collection;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.mapping.neo4j.DeserializationMapper;
import org.apache.flink.mapping.neo4j.Neo4JDeserializationMappingStrategy;

/**
 * @author Alberto De Lazzari
 *
 */
public class GellyNeo4JDeserializationMappingStrategy<V, E> extends
		Neo4JDeserializationMappingStrategy<Tuple2<Collection<Vertex<Long, V>>, Collection<Edge<Long, E>>>, DeserializationMapper<Tuple2<Collection<Vertex<Long, V>>, Collection<Edge<Long, E>>>>> {

	private static final long serialVersionUID = 1L;

	public GellyNeo4JDeserializationMappingStrategy(String templateStatement, GellyDeserializationMapper<V, E> mapper) {
		super(templateStatement, mapper);
	}
}