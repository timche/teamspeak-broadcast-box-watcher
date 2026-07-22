// Preloaded before the test suite (see `bunfig.toml`). `src/config.ts` validates
// `process.env` on import, so the required variables must be present just to load
// the module. Tests import the resulting `config` directly instead of re-declaring
// their own fixtures.
process.env.BROADCAST_BOX_API_URL ??= "http://broadcast-box:8080";
process.env.BROADCAST_BOX_ADMIN_TOKEN ??= "secret";
process.env.PUBLIC_STREAM_HOST ??= "stream.example.com";
process.env.TEAMSPEAK_HOST ??= "teamspeak";
process.env.TEAMSPEAK_QUERY_PASSWORD ??= "pw";
