Embulk::JavaPlugin.register_filter(
  "json_key", "org.embulk.filter.json_key.JsonKeyFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
