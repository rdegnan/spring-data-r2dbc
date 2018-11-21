package org.springframework.data.r2dbc.dialect;

/**
 * An SQL dialect for Postgres.
 *
 * @author Mark Paluch
 */
public class PostgresDialect implements Dialect {

	/**
	 * Singleton instance.
	 */
	public static final PostgresDialect INSTANCE = new PostgresDialect();

	private static final BindMarkersFactory INDEXED = BindMarkersFactory.indexed("$", 1);

	@Override
	public BindMarkersFactory getBindMarkersFactory() {
		return INDEXED;
	}

	@Override
	public String returnGeneratedKeys() {
		return "RETURNING *";
	}
}
