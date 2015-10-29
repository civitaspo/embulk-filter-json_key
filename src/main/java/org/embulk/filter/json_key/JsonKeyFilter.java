package org.embulk.filter.json_key;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.math.NumberUtils;
import org.embulk.filter.json_key.JsonKeyFilterPlugin.PluginTask;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.List;

import static org.embulk.filter.json_key.JsonKeyFilterPlugin.KeyConfig;

/**
 * Created by takahiro.nakayama on 10/28/15.
 */
public class JsonKeyFilter
{
    private final Logger logger = Exec.getLogger(JsonKeyFilter.class);
    private final ObjectMapper mapper = new ObjectMapper();

    private final AddKeyFilter addKeyFilter;
    private final DropKeyFilter dropKeyFilter;

    JsonKeyFilter(PluginTask task)
    {
        this.addKeyFilter = new AddKeyFilter(task.getAddKeyMaps(), task.getNestedKeyDelimiter());
        this.dropKeyFilter = new DropKeyFilter(task.getDropKeyMaps(), task.getNestedKeyDelimiter());
    }

    public String doFilter(String json)
            throws IOException
    {
        JsonNode rootNode = mapper.readTree(json);
        JsonNode filteredNode = addKeyFilter.doFilter(rootNode);
        filteredNode = dropKeyFilter.doFilter(filteredNode);

        return mapper.writeValueAsString(filteredNode);
    }

    private abstract class AbstractKeyFilter
    {
        private final Logger logger = Exec.getLogger(AbstractKeyFilter.class);
        private final List<ImmutableList<String>> keys;
        private final List<JsonNode> values;

        private List<ImmutableList<String>> newKeys(List<KeyConfig> keyConfigs, String delimiter)
        {
            ImmutableList.Builder<ImmutableList<String>> builder = ImmutableList.builder();
            for (KeyConfig keyConfig : keyConfigs) {
                builder.add(ImmutableList.copyOf(Lists.newArrayList(Splitter.on(delimiter).split(keyConfig.getKey()))));
            }
            return builder.build();
        }

        private List<JsonNode> newValues(List<KeyConfig> keyConfigs)
        {
            ImmutableList.Builder<JsonNode> builder = ImmutableList.builder();
            for (KeyConfig keyConfig : keyConfigs) {
                JsonNode value = mapper.getNodeFactory().nullNode();
                if (keyConfig.getValue().isPresent()) {
                    value = mapper.getNodeFactory().pojoNode(keyConfig.getValue().get());
                }
                builder.add(value);
            }
            return builder.build();
        }

        AbstractKeyFilter(List<KeyConfig> keyConfigs, String nestedKeyDelimiter)
        {
            this.keys = newKeys(keyConfigs, nestedKeyDelimiter);
            this.values = newValues(keyConfigs);
        }

        public abstract JsonNode doFilter(JsonNode rootNode);
    }

    private class AddKeyFilter
            extends AbstractKeyFilter
    {
        AddKeyFilter(List<KeyConfig> keyConfigs, String nestedKeyDelimiter)
        {
            super(keyConfigs, nestedKeyDelimiter);
        }

        @Override
        public JsonNode doFilter(JsonNode rootNode)
        {
            JsonNode filteredJsonNode = rootNode;
            for (int i = 0; i < super.keys.size(); i++) {
                filteredJsonNode = addKey(rootNode, super.keys.get(i), super.values.get(i));
            }
            return filteredJsonNode;
        }

        // TODO: addKey is overwriting existing values.
        private JsonNode addKey(JsonNode node, List<String> nestedKey, JsonNode value)
        {
            if (node.isObject()) {
                return addKeyToObject(node, nestedKey, value);
            }
            else if (node.isArray()) {
                return addKeyToArray(node, nestedKey, value);
            }
            // TODO: if NullNode, need to create nodes?
            // else if (node.isNull()) {
            //     return addKeyToNull(node, nestedKey, value);
            // }
            else {
                return node;
            }
        }

        private JsonNode addKeyToObject(JsonNode node, List<String> nestedKey, JsonNode value)
        {
            ObjectNode object = (ObjectNode) node;
            if (nestedKey.isEmpty()) {
                return object;
            }
            else if (nestedKey.size() == 1) {
                object.set(nestedKey.get(0), value);
                return object;
            }
            else {
                String parentKey = nestedKey.get(0);
                List<String> newNestedKey = nestedKey.subList(1, nestedKey.size());
                JsonNode newNode = addKey(object.get(parentKey), newNestedKey, value);
                object.set(parentKey, newNode);
                return object;
            }
        }

        // TODO: addKeyToArray cannot add not existing index.
        private JsonNode addKeyToArray(JsonNode node, List<String> nestedKey, JsonNode value)
        {
            ArrayNode object = (ArrayNode) node;
            if (nestedKey.isEmpty()) {
                return object;
            }
            else if (nestedKey.size() == 1) {
                String key = nestedKey.get(0);
                if (NumberUtils.isNumber(key)) {
                    object.set(Integer.parseInt(key), value);
                }
                return object;
            }
            else {
                String parentIdx = nestedKey.get(0);
                List<String> newNestedKey = nestedKey.subList(1, nestedKey.size());
                if (NumberUtils.isNumber(parentIdx)) {
                    int idx = Integer.parseInt(parentIdx);
                    JsonNode newNode = addKey(object.get(idx), newNestedKey, value);
                    object.set(idx, newNode);
                }
                return object;
            }
        }
    }

    private class DropKeyFilter
            extends AbstractKeyFilter
    {
        DropKeyFilter(List<KeyConfig> keyConfigs, String nestedKeyDelimiter)
        {
            super(keyConfigs, nestedKeyDelimiter);
        }

        @Override
        public JsonNode doFilter(JsonNode rootNode)
        {
            JsonNode filteredJsonNode = rootNode;
            for (List<String> nestedKey : super.keys) {
                filteredJsonNode = dropKey(rootNode, nestedKey);
            }
            return filteredJsonNode;
        }

        // if NullNode has come before end of a nested key, return NullNode not EmptyObjectNode.
        private JsonNode dropKey(JsonNode node, List<String> nestedKey)
        {
            if (node.isObject()) {
                return dropKeyFromObject(node, nestedKey);
            }
            else if (node.isArray()) {
                return dropKeyFromArray(node, nestedKey);
            }
            else {
                return node;
            }
        }

        private JsonNode dropKeyFromObject(JsonNode node, List<String> nestedKey)
        {
            ObjectNode object = (ObjectNode) node;
            if (nestedKey.isEmpty()) {
                return object;
            }
            else if (nestedKey.size() == 1) {
                object.remove(nestedKey.get(0));
                return object;
            }
            else {
                String parentKey = nestedKey.get(0);
                List<String> newNestedKey = nestedKey.subList(1, nestedKey.size());
                JsonNode newNode = dropKey(object.get(parentKey), newNestedKey);
                object.set(parentKey, newNode);
                return object;
            }
        }

        private JsonNode dropKeyFromArray(JsonNode node, List<String> nestedKey)
        {
            ArrayNode object = (ArrayNode) node;
            if (nestedKey.isEmpty()) {
                return object;
            }
            else if (nestedKey.size() == 1) {
                String key = nestedKey.get(0);
                if (NumberUtils.isNumber(key)) {
                    object.remove(Integer.parseInt(key));
                }
                return object;
            }
            else {
                String parentIdx = nestedKey.get(0);
                List<String> newNestedKey = nestedKey.subList(1, nestedKey.size());
                if (NumberUtils.isNumber(parentIdx)) {
                    int idx = Integer.parseInt(parentIdx);
                    JsonNode newNode = dropKey(object.get(idx), newNestedKey);
                    object.set(idx, newNode);
                }
                return object;
            }
        }
    }
}
