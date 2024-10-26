import environ

env = environ.Env()
breakpoint()
print(env("foo", default=None))

DATABASES["default"] = {
    "CONN_MAX_AGE": 600,
    "OPTIONS": {"sslmode": env("POSTGRES_REQUIRE_SSL")},
} | env.db_url(
    "DATABASE_URL",
    default='{"postgis" if cookiecutter.postgis=True else "postgres"}://postgres:postgres@postgres:5432/{{cookiecutter.pg_db}}',
)
