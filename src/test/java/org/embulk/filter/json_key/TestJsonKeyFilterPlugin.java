package org.embulk.filter.json_key;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.filter.json_key.JsonKeyFilterPlugin.PluginTask;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.PageTestUtils;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.json.JSONException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.IOException;

import static org.embulk.spi.FilterPlugin.*;
import static org.embulk.spi.type.Types.STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestJsonKeyFilterPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private final Schema schema = Schema.builder()
            .add("_c0", STRING)
            .build();
    private JsonKeyFilterPlugin filter;

    @Before
    public void createResources()
    {
        filter = new JsonKeyFilterPlugin();
    }

    private ConfigSource getDefaultConfigSource()
    {
        // `column` is required.
        return Exec.newConfigSource().set("column", "_c0");
    }

    private void assertJsonMy(TaskSource taskSource, String expected, String baseData)
    {
        MockPageOutput mockPageOutput = new MockPageOutput();
        PageOutput pageOutput = filter.open(taskSource,
                                            schema,
                                            schema,
                                            mockPageOutput);

        for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(),
                                                 schema,
                                                 baseData)) {
            pageOutput.add(page);
        }

        pageOutput.finish();
        pageOutput.close();

        PageReader pageReader = new PageReader(schema);

        for (Page page : mockPageOutput.pages) {
            pageReader.setPage(page);
            try {
                JSONAssert.assertEquals(expected, pageReader.getString(schema.getColumn(0)), true);
            }
            catch (JSONException e) {
                throw Throwables.propagate(e);
            }
        }
    }


    @Test
    public void testConfigRequiredValues()
    {
        exception.expect(ConfigException.class);
        exception.expectMessage("Field 'column' is required but not set");

        Exec.newConfigSource().loadConfig(PluginTask.class);
    }

    @Test
    public void testConfigDefaultValues()
    {
        PluginTask task = getDefaultConfigSource().loadConfig(PluginTask.class);
        assertTrue(task.getAddKeyConfigs().isEmpty());
        assertTrue(task.getDropKeyConfigs().isEmpty());
        assertEquals(".", task.getNestedKeyDelimiter());
    }


    // Test add_keys: if the value is a YAML Object, this is loaded as a JSON Object.
    @Test
    public void testConfigLoadYamlObjectAsJsonObject()
            throws IOException, JSONException
    {
        String configYaml = "" +
                "type: json_key\n" +
                "column: _c0\n" +
                "add_keys:\n" +
                "  - {key: added1, value: {}}\n" +
                "  - {key: added2, value: {nested: str}}\n" +
                "  - {key: added3, value: {nested: 1}}\n" +
                "  - {key: added4, value: {nested: 2.2}}\n" +
                "  - {key: added5, value: {nested: null}}\n" +
                "  - {key: added6, value: {nested: true}}\n" +
                "  - {key: added7, value: {nested: {}}}\n" +
                "  - {key: added8, value: {nested: {nested: str}}}\n";

        ConfigLoader loader = new ConfigLoader(Exec.getModelManager());
        ConfigSource config = loader.fromYamlString(configYaml);

        filter.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                String jsonData = "{\"c1\":\"value\"}";
                /*
                {
                  "c1": "value"
                }
                */

                String expected = "{\"c1\":\"value\",\"added1\":{},\"added2\":{\"nested\":\"str\"},\"added3\":{\"nested\":1}," +
                        "\"added4\":{\"nested\":2.2},\"added5\":{\"nested\":null},\"added6\":{\"nested\":true}," +
                        "\"added7\":{\"nested\":{}},\"added8\":{\"nested\":{\"nested\":\"str\"}}}}";
                /*
                {
                  "c1": "value",
                  "added1": {
                  },
                  "added2": {
                    "nested": "str"
                  },
                  "added3": {
                    "nested": 1
                  },
                  "added4": {
                    "nested": 2.2
                  },
                  "added5": {
                    "nested": null
                  },
                  "added6": {
                    "nested": true
                  },
                  "added7": {
                    "nested": {}
                  },
                  "added8": {
                    "nested": {
                      "nested": "str"
                    }
                  }
                }
                */

                assertJsonMy(taskSource, expected, jsonData);
            }
        });
    }

    /*
    Test Cases:
        1. Add keys OR Drop Keys
        2. The key already exists OR NOT
        3. Flattened Key OR Single-Nested Key OR Double-Nested Key
     */

    /*
    Case:
        1. Add keys
        2. The key does not already exist
        3. Flattened Key
     */
    @Test
    public void testDoFilterAddNotExistingFlattenedKey()
            throws IOException, JSONException
    {
        ImmutableList.Builder<Object> builder = ImmutableList.builder();
        builder.add(ImmutableMap.of("key", "added1", "value", Optional.of("str")));
        builder.add(ImmutableMap.of("key", "added2", "value", Optional.of(1)));
        builder.add(ImmutableMap.of("key", "added3", "value", Optional.of(2.2)));
        builder.add(ImmutableMap.of("key", "added4", "value", Optional.absent()));
        builder.add(ImmutableMap.of("key", "added5", "value", Optional.of(true)));

        ConfigSource config = getDefaultConfigSource();
        config.set("add_keys", builder.build());

        filter.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                String jsonData = "{\"c1\":\"value\"}";
                /*
                {
                  "c1": "value"
                }
                */

                String expected = "{\"c1\":\"value\",\"added1\":\"str\",\"added2\":1,\"added3\":2.2,\"added4\":null,\"added5\":true}";
                /*
                {
                  "c1": "value",
                  "added1": "str",
                  "added2": 1,
                  "added3": 2.2,
                  "added4": null,
                  "added5": true
                }
                */
                assertJsonMy(taskSource, expected, jsonData);
            }
        });
    }


    /*
    Case:
        1. Add key
        2. The key does not exist
        3. Single-Nested Key
     */
    @Ignore("not ready yet")
    @Test
    public void testDoFilterAddNotExistingSingleNestedKey()
            throws IOException, JSONException
    {
        ImmutableList.Builder<Object> builder = ImmutableList.builder();
        builder.add(ImmutableMap.of("key", "nested1.added1", "value", Optional.of("str")));
        builder.add(ImmutableMap.of("key", "nested1.added2", "value", Optional.of(1)));
        builder.add(ImmutableMap.of("key", "nested1.added3", "value", Optional.of(2.2)));
        builder.add(ImmutableMap.of("key", "nested1.added4", "value", Optional.absent()));
        builder.add(ImmutableMap.of("key", "nested1.added5", "value", Optional.of(true)));
        builder.add(ImmutableMap.of("key", "nested2.1", "value", Optional.of("str")));
        builder.add(ImmutableMap.of("key", "nested2.2", "value", Optional.of(1)));
        builder.add(ImmutableMap.of("key", "nested2.3", "value", Optional.of(2.2)));
        builder.add(ImmutableMap.of("key", "nested2.4", "value", Optional.absent()));
        builder.add(ImmutableMap.of("key", "nested2.5", "value", Optional.of(true)));
        builder.add(ImmutableMap.of("key", "nested2.6", "value", ImmutableMap.of()));
        builder.add(ImmutableMap.of("key", "nested2.7", "value", ImmutableList.of()));

        ConfigSource config = getDefaultConfigSource();
        config.set("add_keys", builder.build());

        filter.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                String jsonData = "{\"c1\":\"value\"}";

                String expected = "{\"c1\":\"value\",\"nested1\":{\"added1\":\"str\",\"added2\":1,\"added3\":2.2,\"added4\":null,\"added5\":true}," + "\"nested2\":[null,\"str\",1,2.2,null,true,{},[]]}";
                /*
                {
                  "c1": "value",
                  "nested1": {
                    "added1": "str",
                    "added2": 1,
                    "added3": 2.2,
                    "added4": null,
                    "added4": true
                  },
                  "nested2":[
                    null,
                    "str",
                    1,
                    2.2,
                    null,
                    true,
                    {},
                    []
                  ]
                }
                */
                assertJsonMy(taskSource, jsonData, expected);
            }
        });
    }

    /*
    Case:
        1. Add key
        2. The key does not exist
        3. Double-Nested Key
     */
    @Ignore("not ready yet")
    @Test
    public void testDoFilterAddNotExistingDoubleNestedKey()
            throws IOException, JSONException
    {
        ImmutableList.Builder<Object> builder = ImmutableList.builder();
        builder.add(ImmutableMap.of("key", "nested1.nested2.added1", "value", Optional.of("str")));
        builder.add(ImmutableMap.of("key", "nested1.nested2.added2", "value", Optional.of(1)));
        builder.add(ImmutableMap.of("key", "nested1.nested2.added3", "value", Optional.of(2.2)));
        builder.add(ImmutableMap.of("key", "nested1.nested2.added4", "value", Optional.absent()));
        builder.add(ImmutableMap.of("key", "nested1.nested2.added5", "value", Optional.of(true)));
        builder.add(ImmutableMap.of("key", "nested1.nested3.5", "value", Optional.of(true)));


        ConfigSource config = getDefaultConfigSource();
        config.set("add_keys", builder.build());

        filter.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                String jsonData = "{\"c1\":\"value\"}";
                String expected = "{\"c1\":\"value\",\"nested1\":{\"nested2\":{" +
                        "\"added1\":\"str\",\"added2\":1,\"added3\":2.2,\"added4\":null,\"added5\":true" +
                        ",\"nested3\": [null,null,null,null,null,true]}}}";
                /*
                {
                  "c1": "value",
                  "nested1": {
                    "nested2": {
                      "added1": "str",
                      "added2": 1,
                      "added3": 2.2,
                      "added4": null,
                      "added4": true
                    },
                    "nested3": [
                      null,
                      null,
                      null,
                      null,
                      null,
                      true
                    ]
                  }
                }
                */
                assertJsonMy(taskSource, expected, jsonData);
            }
        });
    }

    /*
    Case:
        1. Add key
        2. The key exists
        3. Flattened Key
     */
    @Ignore("not ready yet")
    @Test
    public void testDoFilterAddExistingFlattenedKey()
            throws IOException, JSONException
    {
        ImmutableList.Builder<Object> builder = ImmutableList.builder();
        builder.add(ImmutableMap.of("key", "c1", "value", Optional.of("str")));
        builder.add(ImmutableMap.of("key", "added1", "value", Optional.of(1)));
        builder.add(ImmutableMap.of("key", "added1", "value", Optional.of(2.2), "overwrite", true));

        ConfigSource config = getDefaultConfigSource();
        config.set("add_keys", builder.build());

        filter.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                String jsonData = "{\"c1\":\"value\"}";
                String expected = "{\"c1\":\"value\",\"added1\":2.2}";
                /*
                {
                  "c1": "value",
                  "added1": 2.2
                }
                */
                assertJsonMy(taskSource, expected, jsonData);
            }
        });
    }

    /*
    Case:
        1. Add key
        2. The key exists
        3. Single-Nested Key
    */
    @Ignore("not ready yet")
    @Test
    public void testDoFilterAddExistingSingleNestedKey()
            throws IOException, JSONException
    {
        ImmutableList.Builder<Object> builder = ImmutableList.builder();
        builder.add(ImmutableMap.of("key", "c1.nested", "value", Optional.of(1)));
        builder.add(ImmutableMap.of("key", "c2.nested", "value", Optional.of(1), "overwrite", true));
        builder.add(ImmutableMap.of("key", "c3.nested", "value", Optional.of(1)));
        builder.add(ImmutableMap.of("key", "c4.nested", "value", Optional.of(1), "overwrite", true));
        builder.add(ImmutableMap.of("key", "c5.nested", "value", Optional.of(1)));
        builder.add(ImmutableMap.of("key", "c6.nested", "value", Optional.of(1), "overwrite", true));

        ConfigSource config = getDefaultConfigSource();
        config.set("add_keys", builder.build());

        filter.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                String jsonData = "{\"c1\":\"value\",\"c2\":\"value\"," +
                        "\"c3\":{\"nested\":\"value\"},\"c4\":{\"nested\":\"value\"}," +
                        "\"c5\":{\"nested\":{\"nested\":\"value\"}},\"c6\":{\"nested\":{\"nested\":\"value\"}}}";
                /*
                {
                  "c1": "value",
                  "c2": "value",
                  "c3": {
                    "nested": "value"
                  },
                  "c4": {
                    "nested": "value"
                  },
                  "c5": {
                    "nested": {
                      "nested": "value"
                    }
                  },
                  "c6": {
                    "nested": {
                      "nested": "value"
                    }
                  }
                }
                 */
                String expected = "{\"c1\":\"value\",\"c2\":{\"nested\":1}," +
                        "\"c3\":{\"nested\":\"value\"},\"c4\":{\"nested\":1}," +
                        "\"c5\":{\"nested\":{\"nested\":\"value\"}},\"c6\":{\"nested\":1}}";
                /*
                {
                  "c1":"value",
                  "c2":{
                    "nested":1
                  },
                  "c3":{
                    "nested":"value"
                  },
                  "c4":{
                    "nested":1
                  },
                  "c5":{
                    "nested":{
                      "nested":"value"
                    }
                  },
                  "c6":{
                    "nested":1
                  }
                }
                */
                assertJsonMy(taskSource, expected, jsonData);
            }
        });
    }

    /*
    Case:
        1. Add key
        2. The key exists
        3. Double-Nested Key
    */
    @Ignore("not ready yet")
    @Test
    public void testDoFilterAddExistingDoubleNestedKey()
            throws IOException, JSONException
    {
        ImmutableList.Builder<Object> builder = ImmutableList.builder();
        builder.add(ImmutableMap.of("key", "c1.nested.nested", "value", Optional.of(1)));
        builder.add(ImmutableMap.of("key", "c2.nested.nested", "value", Optional.of(1), "overwrite", true));
        builder.add(ImmutableMap.of("key", "c3.nested.nested", "value", Optional.of(1)));
        builder.add(ImmutableMap.of("key", "c4.nested.nested", "value", Optional.of(1), "overwrite", true));
        builder.add(ImmutableMap.of("key", "c5.nested.nested", "value", Optional.of(1)));
        builder.add(ImmutableMap.of("key", "c6.nested.nested", "value", Optional.of(1), "overwrite", true));

        ConfigSource config = getDefaultConfigSource();
        config.set("add_keys", builder.build());

        filter.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                String jsonData = "{\"c1\":\"value\",\"c2\":\"value\"," +
                        "\"c3\":{\"nested\":\"value\"},\"c4\":{\"nested\":\"value\"}," +
                        "\"c5\":{\"nested\":{\"nested\":\"value\"}},\"c6\":{\"nested\":{\"nested\":\"value\"}}}";
                /*
                {
                  "c1": "value",
                  "c2": "value",
                  "c3": {
                    "nested": "value"
                  },
                  "c4": {
                    "nested": "value"
                  },
                  "c5": {
                    "nested": {
                      "nested": "value"
                    }
                  },
                  "c6": {
                    "nested": {
                      "nested": "value"
                    }
                  }
                }
                 */
                String expected = "{\"c1\":\"value\",\"c2\":{\"nested\":{\"nested\":1}}," +
                        "\"c3\":{\"nested\":\"value\"},\"c4\":{\"nested\":{\"nested\":1}}," +
                        "\"c5\":{\"nested\":{\"nested\":\"value\"}},\"c6\":{\"nested\":{\"nested\":1}}}";
                /*
                {
                  "c1":"value",
                  "c2":{
                    "nested":{
                      "nested": 1
                    }
                  },
                  "c3":{
                    "nested":"value"
                  },
                  "c4":{
                    "nested":{
                      "nested": 1
                    }
                  },
                  "c5":{
                    "nested":{
                      "nested":"value"
                    }
                  },
                  "c6":{
                    "nested":{
                      "nested": 1
                    }
                  }
                }
                */
                assertJsonMy(taskSource, expected, jsonData);
            }
        });
    }

    /*
    Case:
        1. Drop Keys
        2. The key does not exist
        3. Flattened Key
     */
    @Test
    public void testDoFilterDropNotExistingFlattenedKey()
            throws IOException, JSONException
    {
        ImmutableList.Builder<Object> builder = ImmutableList.builder();
        builder.add(ImmutableMap.of("key", "c1"));

        ConfigSource config = getDefaultConfigSource();
        config.set("drop_keys", builder.build());

        filter.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                String jsonData = "{}";
                /*
                {
                }
                 */
                String expected = "{}";
                /*
                {
                }
                */
                assertJsonMy(taskSource, expected, jsonData);
            }
        });
    }

    /*
    Case:
        1. Drop Keys
        2. The key does not exist
        3. Single-Nested Key
     */
    @Test
    public void testDoFilterDropNotExistingSingleNestedKey()
            throws IOException, JSONException
    {
        ImmutableList.Builder<Object> builder = ImmutableList.builder();
        builder.add(ImmutableMap.of("key", "c1.nested"));

        ConfigSource config = getDefaultConfigSource();
        config.set("drop_keys", builder.build());

        filter.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                String jsonData = "{}";
                /*
                {
                }
                 */
                String expected = "{}";
                /*
                {
                }
                */
                assertJsonMy(taskSource, expected, jsonData);
            }
        });
    }

    /*
Case:
    1. Drop Keys
    2. The key does not exist
    3. Double-Nested Key
 */
    @Test
    public void testDoFilterDropNotExistingDoubleNestedKey()
            throws IOException, JSONException
    {
        ImmutableList.Builder<Object> builder = ImmutableList.builder();
        builder.add(ImmutableMap.of("key", "c1.nested.nested"));

        ConfigSource config = getDefaultConfigSource();
        config.set("drop_keys", builder.build());

        filter.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                String jsonData = "{}";
                /*
                {
                }
                 */
                String expected = "{}";
                /*
                {
                }
                */
                assertJsonMy(taskSource, expected, jsonData);
            }
        });
    }

    /*
    Case:
        1. Drop Keys
        2. The key exists
        3. Flattened Key
     */
    @Test
    public void testDoFilterDropExistingFlattenedKey()
            throws IOException, JSONException
    {
        ImmutableList.Builder<Object> builder = ImmutableList.builder();
        builder.add(ImmutableMap.of("key", "c1"));
        builder.add(ImmutableMap.of("key", "c2"));
        builder.add(ImmutableMap.of("key", "c3"));
        builder.add(ImmutableMap.of("key", "c4"));
        builder.add(ImmutableMap.of("key", "c5"));
        builder.add(ImmutableMap.of("key", "c6"));

        ConfigSource config = getDefaultConfigSource();
        config.set("drop_keys", builder.build());

        filter.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                String jsonData = "{\"c1\":\"value\",\"c2\":\"value\"," +
                        "\"c3\":{\"nested\":\"value\"},\"c4\":{\"nested\":\"value\"}," +
                        "\"c5\":{\"nested\":{\"nested\":\"value\"}},\"c6\":{\"nested\":{\"nested\":\"value\"}}}";
                /*
                {
                  "c1": "value",
                  "c2": null,
                  "c3": {},
                  "c4": {
                    "nested": "value"
                  },
                  "c5": {
                    "nested": {
                      "nested": "value"
                    }
                  }
                }
                 */

                String expected = "{}";
                /*
                {
                }
                */

                assertJsonMy(taskSource, expected, jsonData);
            }
        });
    }

    /*
    Case:
        1. Drop Keys
        2. The key does not exist
        3. Single-Nested Key
     */
    @Test
    public void testDoFilterDropExistingSingleNestedKey()
            throws IOException, JSONException
    {
        ImmutableList.Builder<Object> builder = ImmutableList.builder();
        builder.add(ImmutableMap.of("key", "c1.nested"));
        builder.add(ImmutableMap.of("key", "c2.nested"));
        builder.add(ImmutableMap.of("key", "c3.nested"));
        builder.add(ImmutableMap.of("key", "c4.nested"));
        builder.add(ImmutableMap.of("key", "c5.nested"));
        builder.add(ImmutableMap.of("key", "c6.nested"));
        builder.add(ImmutableMap.of("key", "c7.nested"));
        builder.add(ImmutableMap.of("key", "c8.nested"));


        ConfigSource config = getDefaultConfigSource();
        config.set("drop_keys", builder.build());

        filter.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                String jsonData = "{\"c1\":\"value\",\"c2\":null," +
                        "\"c3\":{},\"c4\":{\"nested\":\"value\"}," +
                        "\"c5\":{\"nested\":null},\"c6\":{\"nested\":\"value\",\"nested2\":\"value\"}," +
                        "\"c7\":{\"nested\":{}},\"c8\":{\"nested\":{\"nested\":\"value\"}}}";
                /*
                {
                  "c1": "value",
                  "c2": null,
                  "c3": {},
                  "c4": {
                    "nested": "value"
                  },
                  "c5": {
                    "nested": null
                  },
                  "c6": {
                    "nested": "value"
                    "nested2": "value"
                  },
                  "c7": {
                    "nested": {}
                  },
                  "c8": {
                    "nested": {
                      "nested": "value"
                    }
                  }
                }
                 */

                String expected = "{\"c1\":\"value\",\"c2\":null,\"c3\":{},\"c4\":{}," +
                        "\"c5\":{},\"c6\":{\"nested2\":\"value\"},\"c7\":{},\"c8\":{}}";
                /*
                {
                  "c1": "value",
                  "c2": null,
                  "c3": {},
                  "c4": {
                  },
                  "c5": {
                  },
                  "c6": {
                    "nested2": "value"
                  },
                  "c7": {
                  },
                  "c8": {
                  }
                }
                */
                assertJsonMy(taskSource, expected, jsonData);
            }
        });
    }

    /*
    Case:
        1. Drop Keys
        2. The key does not exist
        3. Single-Nested Key
     */
    @Test
    public void testDoFilterDropExistingDoubleNestedKey()
            throws IOException, JSONException
    {
        ImmutableList.Builder<Object> builder = ImmutableList.builder();
        builder.add(ImmutableMap.of("key", "c1.nested.nested"));
        builder.add(ImmutableMap.of("key", "c2.nested.nested"));
        builder.add(ImmutableMap.of("key", "c3.nested.nested"));
        builder.add(ImmutableMap.of("key", "c4.nested.nested"));
        builder.add(ImmutableMap.of("key", "c5.nested.nested"));
        builder.add(ImmutableMap.of("key", "c6.nested.nested"));
        builder.add(ImmutableMap.of("key", "c7.nested.nested"));
        builder.add(ImmutableMap.of("key", "c8.nested.nested"));
        builder.add(ImmutableMap.of("key", "c9.nested.nested"));
        builder.add(ImmutableMap.of("key", "c10.nested.nested"));
        builder.add(ImmutableMap.of("key", "c11.nested.nested"));


        ConfigSource config = getDefaultConfigSource();
        config.set("drop_keys", builder.build());

        filter.transaction(config, schema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                String jsonData = "{\"c1\":\"value\",\"c2\":null,\"c3\":{}," +
                        "\"c4\":{\"nested\":\"value\"},\"c5\":{\"nested\":null}," +
                        "\"c6\":{\"nested\":{}},\"c7\":{\"nested\":{\"nested\":null}}," +
                        "\"c8\":{\"nested\":{\"nested\":\"value\"}},\"c9\":{\"nested\":{\"nested\":\"value\",\"nested2\":\"value\"}}," +
                        "\"c10\":{\"nested\":{\"nested\":{}}},\"c11\":{\"nested\":{\"nested\":{\"nested\":{}}}}}";
                /*
                {
                  "c1": "value",
                  "c2": null,
                  "c3": {},
                  "c4": {
                    "nested": "value"
                  },
                  "c5": {
                    "nested": null
                  },
                  "c6": {
                    "nested": {}
                  },
                  "c7": {
                    "nested": {
                      "nested": null
                    }
                  },
                  "c8": {
                    "nested": {
                      "nested": "value"
                    }
                  },
                  "c9": {
                    "nested": {
                      "nested": "value",
                      "nested2": "value"
                    }
                  },
                  "c10": {
                    "nested": {
                      "nested": {}
                    }
                  },
                  "c11": {
                    "nested": {
                      "nested": {
                        "nested": {}
                      }
                    }
                  }
                }
                 */
                String expected = "{\"c1\":\"value\",\"c2\":null,\"c3\":{}," +
                        "\"c4\":{\"nested\":\"value\"},\"c5\":{\"nested\":null}," +
                        "\"c6\":{\"nested\":{}},\"c7\":{\"nested\":{}}," +
                        "\"c8\":{\"nested\":{}},\"c9\":{\"nested\":{\"nested2\":\"value\"}}," +
                        "\"c10\":{\"nested\":{}},\"c11\":{\"nested\":{}}}";
                /*
                {
                  "c1": "value",
                  "c2": null,
                  "c3": {},
                  "c4": {
                    "nested": "value"
                  },
                  "c5": {
                    "nested": null
                  },
                  "c6": {
                    "nested": {}
                  },
                  "c7": {
                    "nested": {
                    }
                  },
                  "c8": {
                    "nested": {
                    }
                  },
                  "c9": {
                    "nested": {
                      "nested2": "value"
                    }
                  },
                  "c10": {
                    "nested": {
                    }
                  },
                  "c11": {
                    "nested": {
                    }
                  }
                }
                */
                assertJsonMy(taskSource, expected, jsonData);
            }
        });
    }
}
