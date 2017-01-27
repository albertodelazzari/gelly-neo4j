/**
 * 
 */
package org.apache.flink.gelly.batch.neo4j;

import java.util.Collection;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.batch.neo4j.Neo4JOutputFormat;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.mapping.neo4j.DeserializationMapper;
import org.apache.flink.mapping.neo4j.Neo4JDeserializationMappingStrategy;

/**
 * @author Alberto De Lazzari
 *
 */
public class GellyNeo4JOutputFormat<V extends Map<String, Object>, E extends Map<String, Object>>
		extends Neo4JOutputFormat<Tuple2<Collection<Vertex<Long, V>>, Collection<Edge<Long, E>>>> {

	private static final long serialVersionUID = 1L;

	/**
	 * 
	 * @param mappingStrategy
	 * @param config
	 */
	public GellyNeo4JOutputFormat(
			Neo4JDeserializationMappingStrategy<Tuple2<Collection<Vertex<Long, V>>, Collection<Edge<Long, E>>>, DeserializationMapper<Tuple2<Collection<Vertex<Long, V>>, Collection<Edge<Long, E>>>>> mappingStrategy,
			Map<String, String> config) {
		super(mappingStrategy, config);
	}

}
