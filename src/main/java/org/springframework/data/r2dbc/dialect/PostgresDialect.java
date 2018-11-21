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

	private static final LimitClause LIMIT_CLAUSE = new LimitClause() {

		@Override
		public String getClause(long limit) {
			return "LIMIT " + limit;
		}

		@Override
		public Position getClausePosition() {
			return Position.END;
		}
	};

	@Override
	public BindMarkersFactory getBindMarkersFactory() {
		return INDEXED;
	}

	@Override
	public String returnGeneratedKeys() {
		return "RETURNING *";
	}

	@Override
	public LimitClause limit() {
		return LIMIT_CLAUSE;
	}
}
