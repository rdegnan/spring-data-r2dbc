/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.r2dbc.function;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.r2dbc.dialect.BindMarker;
import org.springframework.data.r2dbc.dialect.BindMarkers;
import org.springframework.data.r2dbc.dialect.Dialect;
import org.springframework.data.r2dbc.function.convert.EntityRowMapper;
import org.springframework.data.r2dbc.function.convert.SettableValue;
import org.springframework.data.relational.core.conversion.BasicRelationalConverter;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.util.StreamUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Default {@link ReactiveDataAccessStrategy} implementation.
 *
 * @author Mark Paluch
 */
public class DefaultReactiveDataAccessStrategy implements ReactiveDataAccessStrategy {

	private final RelationalConverter relationalConverter;
	private final Dialect dialect;

	public DefaultReactiveDataAccessStrategy(Dialect dialect) {
		this(dialect, new BasicRelationalConverter(new RelationalMappingContext()));
	}

	public DefaultReactiveDataAccessStrategy(Dialect dialect, RelationalConverter converter) {
		this.relationalConverter = converter;
		this.dialect = dialect;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#getAllFields(java.lang.Class)
	 */
	@Override
	public List<String> getAllFields(Class<?> typeToRead) {

		RelationalPersistentEntity<?> persistentEntity = getPersistentEntity(typeToRead);

		if (persistentEntity == null) {
			return Collections.singletonList("*");
		}

		return StreamUtils.createStreamFromIterator(persistentEntity.iterator()) //
				.map(RelationalPersistentProperty::getColumnName) //
				.collect(Collectors.toList());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#getValuesToInsert(java.lang.Object)
	 */
	@Override
	public List<SettableValue> getValuesToInsert(Object object) {

		Class<?> userClass = ClassUtils.getUserClass(object);

		RelationalPersistentEntity<?> entity = getRequiredPersistentEntity(userClass);
		PersistentPropertyAccessor propertyAccessor = entity.getPropertyAccessor(object);

		List<SettableValue> values = new ArrayList<>();

		for (RelationalPersistentProperty property : entity) {

			Object value = propertyAccessor.getProperty(property);

			if (value == null) {
				continue;
			}

			values.add(new SettableValue(property.getColumnName(), value, property.getType()));
		}

		return values;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#getMappedSort(java.lang.Class, org.springframework.data.domain.Sort)
	 */
	@Override
	public Sort getMappedSort(Class<?> typeToRead, Sort sort) {

		RelationalPersistentEntity<?> entity = getPersistentEntity(typeToRead);
		if (entity == null) {
			return sort;
		}

		List<Order> mappedOrder = new ArrayList<>();

		for (Order order : sort) {

			RelationalPersistentProperty persistentProperty = entity.getPersistentProperty(order.getProperty());
			if (persistentProperty == null) {
				mappedOrder.add(order);
			} else {
				mappedOrder
						.add(Order.by(persistentProperty.getColumnName()).with(order.getNullHandling()).with(order.getDirection()));
			}
		}

		return Sort.by(mappedOrder);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#getRowMapper(java.lang.Class)
	 */
	@Override
	public <T> BiFunction<Row, RowMetadata, T> getRowMapper(Class<T> typeToRead) {
		return new EntityRowMapper<T>((RelationalPersistentEntity) getRequiredPersistentEntity(typeToRead),
				relationalConverter);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#getTableName(java.lang.Class)
	 */
	@Override
	public String getTableName(Class<?> type) {
		return getRequiredPersistentEntity(type).getTableName();
	}

	private RelationalPersistentEntity<?> getRequiredPersistentEntity(Class<?> typeToRead) {
		return relationalConverter.getMappingContext().getRequiredPersistentEntity(typeToRead);
	}

	@Nullable
	private RelationalPersistentEntity<?> getPersistentEntity(Class<?> typeToRead) {
		return relationalConverter.getMappingContext().getPersistentEntity(typeToRead);
	}

	@Override
	public BindableOperation insertAndReturnGeneratedKeys(String table, Set<String> columns) {
		return new DefaultBindableInsert(dialect.getBindMarkersFactory().create(), table, columns,
				dialect.returnGeneratedKeys());
	}

	/**
	 * Default {@link BindableOperation} implementation for a {@code INSERT} operation.
	 */
	static class DefaultBindableInsert implements BindableOperation {

		private final Map<String, BindMarker> markers = new LinkedHashMap<>();
		private final String query;

		DefaultBindableInsert(BindMarkers bindMarkers, String table, Collection<String> columns,
				String returningStatement) {

			StringBuilder builder = new StringBuilder();
			List<String> placeholders = new ArrayList<>(columns.size());

			for (String column : columns) {
				BindMarker marker = markers.computeIfAbsent(column, bindMarkers::next);
				placeholders.add(marker.getPlaceholder());
			}

			String columnsString = StringUtils.collectionToDelimitedString(columns, ",");
			String placeholdersString = StringUtils.collectionToDelimitedString(placeholders, ",");

			builder.append("INSERT INTO ").append(table).append(" (").append(columnsString).append(")").append(" VALUES(")
					.append(placeholdersString).append(")");

			if (StringUtils.hasText(returningStatement)) {
				builder.append(' ').append(returningStatement);
			}

			this.query = builder.toString();
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.BindableOperation#bind(io.r2dbc.spi.Statement, java.lang.String, java.lang.Object)
		 */
		@Override
		public void bind(Statement<?> statement, String identifier, Object value) {
			markers.get(identifier).bind(statement, value);
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.BindableOperation#bindNull(io.r2dbc.spi.Statement, java.lang.String, java.lang.Class)
		 */
		@Override
		public void bindNull(Statement<?> statement, String identifier, Class<?> valueType) {
			markers.get(identifier).bindNull(statement, valueType);
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.QueryOperation#toQuery()
		 */
		@Override
		public String toQuery() {
			return this.query;
		}
	}
}
