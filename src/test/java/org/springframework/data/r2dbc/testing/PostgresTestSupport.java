package org.springframework.data.r2dbc.testing;

/**
 * Utility class for testing against Postgres.
 *
 * @author Mark Paluch
 */
public class PostgresTestSupport {

	public static String CREATE_TABLE_LEGOSET = "CREATE TABLE IF NOT EXISTS legoset (\n" //
			+ "    id          integer CONSTRAINT id PRIMARY KEY,\n" //
			+ "    name        varchar(255) NOT NULL,\n" //
			+ "    manual      integer NULL\n" //
			+ ");";

	public static String INSERT_INTO_LEGOSET = "INSERT INTO legoset (id, name, manual) VALUES($1, $2, $3)";
}
