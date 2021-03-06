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

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.domain.Sort.Order.*;

import io.r2dbc.spi.ConnectionFactory;
import lombok.Data;
import reactor.core.publisher.Hooks;
import reactor.test.StepVerifier;

import org.junit.Before;
import org.junit.Test;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.testing.R2dbcIntegrationTestSupport;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Integration tests for {@link DatabaseClient} against PostgreSQL.
 *
 * @author Mark Paluch
 */
public class DatabaseClientIntegrationTests extends R2dbcIntegrationTestSupport {

	private ConnectionFactory connectionFactory;

	private JdbcTemplate jdbc;

	@Before
	public void before() {

		Hooks.onOperatorDebug();

		connectionFactory = createConnectionFactory();

		String tableToCreate = "CREATE TABLE IF NOT EXISTS legoset (\n"
				+ "    id          integer CONSTRAINT id PRIMARY KEY,\n" + "    name        varchar(255) NOT NULL,\n"
				+ "    manual      integer NULL\n" + ");";

		jdbc = createJdbcTemplate(createDataSource());
		jdbc.execute(tableToCreate);
		jdbc.execute("DELETE FROM legoset");
	}

	@Test
	public void executeInsert() {

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.execute().sql("INSERT INTO legoset (id, name, manual) VALUES($1, $2, $3)") //
				.bind(0, 42055) //
				.bind(1, "SCHAUFELRADBAGGER") //
				.bindNull("$3", Integer.class) //
				.fetch().rowsUpdated() //
				.as(StepVerifier::create) //
				.expectNext(1) //
				.verifyComplete();

		assertThat(jdbc.queryForMap("SELECT id, name, manual FROM legoset")).containsEntry("id", 42055);
	}

	@Test
	public void shouldTranslateDuplicateKeyException() {

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		executeInsert();

		databaseClient.execute().sql("INSERT INTO legoset (id, name, manual) VALUES($1, $2, $3)") //
				.bind(0, 42055) //
				.bind(1, "SCHAUFELRADBAGGER") //
				.bindNull("$3", Integer.class) //
				.fetch().rowsUpdated() //
				.as(StepVerifier::create) //
				.expectErrorSatisfies(exception -> {

					assertThat(exception).isInstanceOf(DuplicateKeyException.class)
							.hasMessageContaining("execute; SQL [INSERT INTO legoset");
				}) //
				.verify();
	}

	@Test
	public void executeSelect() {

		jdbc.execute("INSERT INTO legoset (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		// TODO: Driver/Decode does not support decoding null values?
		databaseClient.execute().sql("SELECT id, name, manual FROM legoset") //
				.as(LegoSet.class) //
				.fetch().all() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.getId()).isEqualTo(42055);
					assertThat(actual.getName()).isEqualTo("SCHAUFELRADBAGGER");
					assertThat(actual.getManual()).isEqualTo(12);
				}).verifyComplete();
	}

	@Test
	public void insert() {

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.insert().into("legoset")//
				.value("id", 42055) //
				.value("name", "SCHAUFELRADBAGGER") //
				.nullValue("manual", Integer.class) //
				.exchange() //
				.flatMapMany(it -> it.extract((r, m) -> r.get("id", Integer.class)).all()) //
				.as(StepVerifier::create) //
				.expectNext(42055).verifyComplete();

		assertThat(jdbc.queryForMap("SELECT id, name, manual FROM legoset")).containsEntry("id", 42055);
	}

	@Test
	public void insertWithoutResult() {

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.insert().into("legoset")//
				.value("id", 42055) //
				.value("name", "SCHAUFELRADBAGGER") //
				.nullValue("manual", Integer.class) //
				.then() //
				.as(StepVerifier::create) //
				.verifyComplete();

		assertThat(jdbc.queryForMap("SELECT id, name, manual FROM legoset")).containsEntry("id", 42055);
	}

	@Test
	public void insertTypedObject() {

		LegoSet legoSet = new LegoSet();
		legoSet.setId(42055);
		legoSet.setName("SCHAUFELRADBAGGER");
		legoSet.setManual(12);

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.insert().into(LegoSet.class)//
				.using(legoSet).exchange() //
				.flatMapMany(it -> it.extract((r, m) -> r.get("id", Integer.class)).all()).as(StepVerifier::create) //
				.expectNext(42055).verifyComplete();

		assertThat(jdbc.queryForMap("SELECT id, name, manual FROM legoset")).containsEntry("id", 42055);
	}

	@Test
	public void select() {

		jdbc.execute("INSERT INTO legoset (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.select().from(LegoSet.class) //
				.project("id", "name", "manual") //
				.orderBy(Sort.by("id")) //
				.fetch().all() //
				.as(StepVerifier::create) //
				.assertNext(actual -> {
					assertThat(actual.getId()).isEqualTo(42055);
					assertThat(actual.getName()).isEqualTo("SCHAUFELRADBAGGER");
					assertThat(actual.getManual()).isEqualTo(12);
				}).verifyComplete();
	}

	@Test
	public void selectOrderByIdDesc() {

		jdbc.execute("INSERT INTO legoset (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");
		jdbc.execute("INSERT INTO legoset (id, name, manual) VALUES(42064, 'FORSCHUNGSSCHIFF', 13)");
		jdbc.execute("INSERT INTO legoset (id, name, manual) VALUES(42068, 'FLUGHAFEN-LÖSCHFAHRZEUG', 13)");

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.select().from(LegoSet.class) //
				.orderBy(Sort.by(desc("id"))) //
				.fetch().all() //
				.map(LegoSet::getId) //
				.as(StepVerifier::create) //
				.expectNext(42068, 42064, 42055) //
				.verifyComplete();
	}

	@Test
	public void selectOrderPaged() {

		jdbc.execute("INSERT INTO legoset (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");
		jdbc.execute("INSERT INTO legoset (id, name, manual) VALUES(42064, 'FORSCHUNGSSCHIFF', 13)");
		jdbc.execute("INSERT INTO legoset (id, name, manual) VALUES(42068, 'FLUGHAFEN-LÖSCHFAHRZEUG', 13)");

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.select().from(LegoSet.class) //
				.orderBy(Sort.by(desc("id"))) //
				.page(PageRequest.of(1, 1)).fetch().all() //
				.map(LegoSet::getId) //
				.as(StepVerifier::create) //
				.expectNext(42064) //
				.verifyComplete();
	}

	@Test
	public void selectTypedLater() {

		jdbc.execute("INSERT INTO legoset (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");
		jdbc.execute("INSERT INTO legoset (id, name, manual) VALUES(42064, 'FORSCHUNGSSCHIFF', 13)");
		jdbc.execute("INSERT INTO legoset (id, name, manual) VALUES(42068, 'FLUGHAFEN-LÖSCHFAHRZEUG', 13)");

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.select().from("legoset") //
				.orderBy(Sort.by(desc("id"))) //
				.as(LegoSet.class) //
				.fetch().all() //
				.map(LegoSet::getId) //
				.as(StepVerifier::create) //
				.expectNext(42068, 42064, 42055) //
				.verifyComplete();
	}

	@Data
	@Table("legoset")
	static class LegoSet {
		int id;
		String name;
		Integer manual;
	}
}
