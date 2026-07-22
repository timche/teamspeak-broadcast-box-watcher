// Preloaded before the test suite (see `bunfig.toml`). `src/config.ts` validates
// `process.env` on import, so tests need the required variables present just to
// load the module; individual tests still build their own config via
// `configSchema.parse(...)`.
process.env.BROADCAST_BOX_API_URL ??= "http://broadcast-box:8080";
process.env.BROADCAST_BOX_ADMIN_TOKEN ??= "test-token";
process.env.PUBLIC_STREAM_HOST ??= "stream.example.com";
process.env.TEAMSPEAK_HOST ??= "teamspeak";
process.env.TEAMSPEAK_QUERY_PASSWORD ??= "test-password";
