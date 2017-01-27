/**
 * 
 */
package org.apache.flink.gelly.mapping.neo4j;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.gelly.mapping.neo4j.exception.ValueConversionException;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.internal.value.BooleanValue;
import org.neo4j.driver.internal.value.FloatValue;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Alberto De Lazzari
 *
 */
public class GellyDeserializationDefaultMapper<V extends Map<String, Object>, E extends Map<String, Object>>
		implements GellyDeserializationMapper<V, E> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOGGER = LoggerFactory.getLogger(GellyDeserializationDefaultMapper.class);

	private final InternalValueMapper internalValueMapper = new InternalValueMapper();

	private static final String LABEL_KEY = "labels";
	
	private static final String TYPE_KEY = "type";
	
	public GellyDeserializationDefaultMapper() {
		super();
	}

	@Override
	public Map<String, Object> deserialize(Tuple2<Collection<Vertex<Long, V>>, Collection<Edge<Long, E>>> item) {
		// First we create all the nodes
		Collection<Vertex<Long, V>> vertices = item.f0;
		Collection<Edge<Long, E>> edges = item.f1;

		//List<Node> nodes = vertices.stream().map(this::vertexToNode).collect(Collectors.toList());
		List<Map<String, Object>> nodes = vertices.stream().map(this::vertexToMap).collect(Collectors.toList());
		LOGGER.debug("Nodes: {}", nodes);

		//List<Relationship> relationships = edges.stream().map(this::edgeToRelationship).collect(Collectors.toList());
		List<Map<String, Object>> relationships = edges.stream().map(this::edgeToMap).collect(Collectors.toList());
		LOGGER.debug("Relationships: {}", relationships);

		// It returns nodes and relationships as two separated sets. It will be
		// better if we return a Path

		Map<String, Object> entities = new HashMap<>();
		entities.put("nodes", nodes);
		entities.put("relationships", relationships);

		return entities;
	}

	/**
	 * 
	 * @param v
	 * @return
	 */
	private Map<String, Object> vertexToMap(Vertex<Long, V> v) {
		Map<String, Object> properties = new HashMap<>(v.f1);
		properties.put("id", v.f0);
		
		if (!properties.containsKey(LABEL_KEY)) {
			properties.put(LABEL_KEY, Collections.singleton("VERTEX"));
		}

		return properties;
	}
	
	/**
	 * 
	 * @param e
	 * @return
	 */
	private Map<String, Object> edgeToMap(Edge<Long, E> e){
		Map<String, Object> properties = new HashMap<>(e.f2);
		properties.put("id", e.f0 ^ e.f1 >>> 32);
		properties.put("startNode", e.f0);
		properties.put("endNode", e.f1);
		
		if (!properties.containsKey(TYPE_KEY)) {
			properties.put(TYPE_KEY, Collections.singleton("EDGE"));
		}
		
		return properties;
	}

	/**
	 * 
	 * @param v
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "unused" })
	private Node vertexToNode(Vertex<Long, V> v) {
		Map<String, Value> properties = v.f1.entrySet().stream()
				.collect(Collectors.toMap(Entry::getKey, internalValueMapper::getInternalValue));

		// We search for a property "labels" that will be converted in a
		// list of labels for the neo4j node
		List<String> nodeLabels = new ArrayList<>();

		Object labels = v.f1.get(LABEL_KEY);
		// The property "labels" should exist and should be a collection of
		// string
		if (labels != null && List.class.isAssignableFrom(labels.getClass())) {
			nodeLabels.addAll((List<String>) labels);
		} else {
			nodeLabels.add("VERTEX");
		}

		return new InternalNode(v.f0.longValue(), nodeLabels, properties);
	}

	/**
	 * 
	 * @param e
	 * @return
	 */
	@SuppressWarnings("unused")
	private Relationship edgeToRelationship(Edge<Long, E> e) {
		Map<String, Value> properties = e.f2.entrySet().stream()
				.collect(Collectors.toMap(Entry::getKey, internalValueMapper::getInternalValue));

		// We search for a property "type" that will be converted in a
		// string for the neo4j relationship
		String relationshipType = "EDGE";

		Object type = e.f2.get(TYPE_KEY);
		// The property "type" should exist and should be a string
		if (type != null) {
			relationshipType = type.toString();
		}

		return new InternalRelationship((e.f0.toString() + e.f1.toString()).hashCode(), e.f0, e.f1, relationshipType,
				properties);
	}

	final class InternalValueMapper implements Serializable {

		private static final long serialVersionUID = 1L;

		private final Map<Class<?>, Class<? extends Value>> typeMapper = new HashMap<>();

		public InternalValueMapper() {
			super();
			typeMapper.put(Boolean.class, BooleanValue.class);
			typeMapper.put(Float.class, FloatValue.class);
			typeMapper.put(Double.class, FloatValue.class);
			typeMapper.put(Integer.class, IntegerValue.class);
			typeMapper.put(Long.class, IntegerValue.class);
			typeMapper.put(List.class, ListValue.class);
			typeMapper.put(String.class, StringValue.class);
		}

		private Class<? extends Value> resolveClass(Class<?> clazz) {
			Class<? extends Value> internalClassValue = typeMapper.get(clazz);

			if (internalClassValue == null) {
				for (Entry<Class<?>, Class<? extends Value>> entry : typeMapper.entrySet()) {
					if (entry.getKey().isAssignableFrom(clazz)) {
						return entry.getValue();
					}
				}
			}

			return internalClassValue;
		}

		public final Value getInternalValue(Entry<String, Object> entry) {
			Object value = entry.getValue();
			return getInternalValue(value);
		}

		public final Value getInternalValue(Object value) {
			// The type mapper should be smarter!!
			Class<? extends Value> internalClassValue = resolveClass(value.getClass());

			if (internalClassValue == null) {
				throw new ValueConversionException("Internal class resolution failure for " + value.getClass());
			}

			try {
				if (!internalClassValue.equals(ListValue.class)) {
					return (Value) internalClassValue.getConstructors()[0].newInstance(value);
				}

				List<?> valuesToConvert = (List<?>) value;
				Value[] internalValue = new Value[valuesToConvert.size()];
				int i = 0;
				for (Object valueToConvert : valuesToConvert) {
					internalValue[i] = getInternalValue(valueToConvert);
					i++;
				}

				return internalClassValue.getConstructor(internalValue.getClass()).newInstance((Object) internalValue);
			} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException | SecurityException | NoSuchMethodException e) {
				LOGGER.error(e.getLocalizedMessage(), e);
				return null;
			}
		}
	}
}
