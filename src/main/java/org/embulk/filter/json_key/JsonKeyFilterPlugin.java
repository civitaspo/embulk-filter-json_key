package org.embulk.filter.json_key;

import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.slf4j.Logger;

import java.util.List;

public class JsonKeyFilterPlugin
        implements FilterPlugin
{
    public interface KeyConfig
            extends Task
    {
        @Config("key")
        public String getKey();

        @Config("value")
        @ConfigDefault("null")
        public Optional<Object> getValue();
    }

    public interface PluginTask
            extends Task
    {
        @Config("column")
        public String getColumnName();

        @Config("nested_key_delimiter")
        @ConfigDefault("\".\"")
        public String getNestedKeyDelimiter();

        @Config("add_keys")
        @ConfigDefault("[]")
        public List<KeyConfig> getAddKeyConfigs();

        @Config("drop_keys")
        @ConfigDefault("[]")
        public List<KeyConfig> getDropKeyConfigs();
    }

    private final Logger logger = Exec.getLogger(JsonKeyFilterPlugin.class);

    @Override
    public void transaction(ConfigSource config, Schema inputSchema, FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema outputSchema = inputSchema;

        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, Schema inputSchema, Schema outputSchema, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        return new FilteredPageOutput(task, inputSchema, outputSchema, output);
    }
}
