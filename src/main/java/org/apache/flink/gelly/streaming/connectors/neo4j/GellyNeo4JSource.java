/**
 * 
 */
package org.apache.flink.gelly.streaming.connectors.neo4j;

import java.util.Collection;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.gelly.mapping.neo4j.GellyNeo4JSerializationMappingStrategy;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.streaming.connectors.neo4j.Neo4JSource;

/**
 * @author Alberto De Lazzari
 *
 */
public class GellyNeo4JSource<V extends Map<String, Object>, E>
		extends Neo4JSource<Tuple2<Collection<Vertex<Long, V>>, Collection<Edge<Long, E>>>> {

	private static final long serialVersionUID = 1L;

	public GellyNeo4JSource(GellyNeo4JSerializationMappingStrategy<V, E> mappingStrategy, Map<String, String> config) {
		super(mappingStrategy, config);
	}
}