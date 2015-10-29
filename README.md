# Json Key filter plugin for Embulk

Filtering keys from JSON objects.

## Overview

* **Plugin type**: filter

## Configuration

- **column**: column name of JSON (string, required)
- **nested_key_delimiter**: delimiter to express nested JSON keys (string, required)
- **add_keys**: JSON keys to add (array of hash, optional)
  - **key**: key name to add (string)
  - **value**: value of the key (anything)
- **drop_keys**: JSON keys to drop (array of string, optional)
  - **key**: key name to add (string)
  
## Example Config

```yaml
filters:
  - type: json_key
    column: json_payload
    nested_key_delimiter: "."
    add_keys:
      - {key: k1} # value is null
      - {key: k2.k2, value: test_string}
      - {key: k3.3, value: {test: test_object}}
    drop_keys:
      - {key: k5}
      - {key: k6.k6-1}
      - {key: k7.3}
```

## Example(add_keys)
Say input.tsv is as follows:

```
json_payload
{"phone_numbers":"1-276-220-7263","profile":{"like_words":["maiores","eum","aut"],"anniversary":{"voluptatem":"dolor","et":"ullam"}}}
{"phone_numbers":"553.980.4072","profile":{"like_words":["nobis","ad","est"],"anniversary": null}}
{"phone_numbers":"267-437-9081","profile":{"like_words":["itaque","aut","in"],"anniversary":{"eveniet":"in","id":"sit"}}}
{"phone_numbers":"639.217.7325","profile":{"like_words":["a","molestiae","iure"],"anniversary":{"non":"harum","dolorem":"provident"}}}
{"phone_numbers":"590.289.2473","profile":{"like_words":["quisquam","quasi","a"],"anniversary":{"ducimus":"veritatis","vel":"in"}}}
{"phone_numbers":"(196) 116-8976","profile":{"like_words":["maxime","ad","sunt"],"anniversary":{"molestiae":"architecto","temporibus":"quia"}}}
```
```yaml
filters:
  - type: json_key
    column: json_payload
    nested_key_delimiter: "."
    add_keys:
      - {key: add_key1}
      - {key: add_key2, value: add_value2}
      - {key: add_key3, value: 3}
      - {key: add_key4, value: [1,2,3,4]}
      - {key: add_key5, value: {key1: value, key2: 2, key3: {key: value}}}
      - {key: profile.add_key6, value: add_value6}
```

The result is:

```
{"phone_numbers":"1-276-220-7263","profile":{"like_words":["maiores","eum","aut"],"anniversary":{"voluptatem":"dolor","et":"ullam"},"add_key6":"add_value6"},"add_key1":null,"add_key2":"add_value2","add_key3":3,"add_key4":[1,2,3,4],"add_key5":{"key1":"value","key2":2,"key3":{"key":"value"}}}
{"phone_numbers":"553.980.4072","profile":{"like_words":["nobis","ad","est"],"anniversary":null,"add_key6":"add_value6"},"add_key1":null,"add_key2":"add_value2","add_key3":3,"add_key4":[1,2,3,4],"add_key5":{"key1":"value","key2":2,"key3":{"key":"value"}}}
{"phone_numbers":"267-437-9081","profile":{"like_words":["itaque","aut","in"],"anniversary":{"eveniet":"in","id":"sit"},"add_key6":"add_value6"},"add_key1":null,"add_key2":"add_value2","add_key3":3,"add_key4":[1,2,3,4],"add_key5":{"key1":"value","key2":2,"key3":{"key":"value"}}}
{"phone_numbers":"639.217.7325","profile":{"like_words":["a","molestiae","iure"],"anniversary":{"non":"harum","dolorem":"provident"},"add_key6":"add_value6"},"add_key1":null,"add_key2":"add_value2","add_key3":3,"add_key4":[1,2,3,4],"add_key5":{"key1":"value","key2":2,"key3":{"key":"value"}}}
{"phone_numbers":"590.289.2473","profile":{"like_words":["quisquam","quasi","a"],"anniversary":{"ducimus":"veritatis","vel":"in"},"add_key6":"add_value6"},"add_key1":null,"add_key2":"add_value2","add_key3":3,"add_key4":[1,2,3,4],"add_key5":{"key1":"value","key2":2,"key3":{"key":"value"}}}
{"phone_numbers":"(196) 116-8976","profile":{"like_words":["maxime","ad","sunt"],"anniversary":{"molestiae":"architecto","temporibus":"quia"},"add_key6":"add_value6"},"add_key1":null,"add_key2":"add_value2","add_key3":3,"add_key4":[1,2,3,4],"add_key5":{"key1":"value","key2":2,"key3":{"key":"value"}}}
```



### Example(drop_keys)
Say input.tsv is as follows:

```
json_payload
{"phone_numbers":"1-276-220-7263","profile":{"like_words":["maiores","eum","aut"],"anniversary":{"voluptatem":"dolor","et":"ullam"}}}
{"phone_numbers":"553.980.4072","profile":{"like_words":["nobis","ad","est"],"anniversary": null}}
{"phone_numbers":"267-437-9081","profile":{"like_words":["itaque","aut","in"],"anniversary":{"eveniet":"in","id":"sit"}}}
{"phone_numbers":"639.217.7325","profile":{"like_words":["a","molestiae","iure"],"anniversary":{"non":"harum","dolorem":"provident"}}}
{"phone_numbers":"590.289.2473","profile":{"like_words":["quisquam","quasi","a"],"anniversary":{"ducimus":"veritatis","vel":"in"}}}
{"phone_numbers":"(196) 116-8976","profile":{"like_words":["maxime","ad","sunt"],"anniversary":{"molestiae":"architecto","temporibus":"quia"}}}
```
```yaml
filters:
  - type: json_key
    column: json_payload
    nested_key_delimiter: "."
    drop_keys:
      - {key: phone_numbers}
      - {key: profile.like_words.2}
      - {key: profile.anniversary.voluptatem}
```

The result is:

```
{"profile":{"like_words":["maiores","eum"],"anniversary":{"et":"ullam"}}}
{"profile":{"like_words":["nobis","ad"],"anniversary":null}}
{"profile":{"like_words":["itaque","aut"],"anniversary":{"eveniet":"in","id":"sit"}}}
{"profile":{"like_words":["a","molestiae"],"anniversary":{"non":"harum","dolorem":"provident"}}}
{"profile":{"like_words":["quisquam","quasi"],"anniversary":{"ducimus":"veritatis","vel":"in"}}}
{"profile":{"like_words":["maxime","ad"],"anniversary":{"molestiae":"architecto","temporibus":"quia"}}}
```

## TODO

- **add_keys** is overwriting existing values. need to **overwrite** option.
- **add_keys** cannot add keys array nodes.
- write tests

## Run Example

```
$ ./gradlew classpath
$ embulk run -Ilib example config.yml
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
