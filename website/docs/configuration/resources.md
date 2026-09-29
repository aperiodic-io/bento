---
title: Resources
---

Resources are components within Bento that are declared with a unique label and can be referenced any number of times within a configuration. Only one instance of each named resource is created, but it is safe to use it in multiple places as they can be shared without consequence.

Some components such as caches and rate limits can _only_ be created as a resource. However, for components where it's optional there are a few reasons why it might be advantageous to do so.

```yaml
input:
  resource: foo

pipeline:
  processors:
    - resource: bar
    - cache:
        operator: set
        resource: baz
        key: ${! json("id") }
        value: ${! content() }

output:
  resource: buz

input_resources:
  - label: foo
    file:
      paths: [ ./in.txt ]

processor_resources:
  - label: bar
    mapping: 'root = content.lowercase()'

cache_resources:
  - label: baz
    memory:
      default_ttl: 300s

output_resources:
  - label: buz
    file:
      path: ./out.txt
```

## Reusability

Sometimes it's necessary to use a rather large component multiple times. Instead of copy/pasting the configuration or using YAML anchors you can define your component as a resource.

In the following example we want to look up a value from a cache using the contents of our payloads as the key. Occasionally the lookup might fail due to garbage within the contents, and so we catch these failed lookups, attempt to "cleanse" the contents and try the same lookup again. Since the lookup component is likely to change over time we make sure to avoid duplicating it by defining it as a resource `get_foo`:

```yaml
pipeline:
  processors:
    - resource: get_foo
    - catch:
      - mapping: |
          root = this
          root.content = this.content.strip_html()
      - resource: get_foo

processor_resources:
  - label: get_foo
    cache:
      resource: foo_cache
      operator: get
      key: '${! json("content") }'
```

## Feature Toggling

### With Environment Variables

There are two ways of using resources for feature toggling, the first is to define your feature components with unique names and then apply the old switcheroo with environment variables to select the one you wish to execute:

```yaml
pipeline:
  processors:
    - resource: ${FEATURE_REQUEST}

processor_resources:
  - label: get_foo
    cache:
      resource: foo_cache
      operator: get
      key: '${! json("content") }'

  - label: get_bar
    cache:
      resource: bar_cache
      operator: get
      key: '${! json("id") }'
```

Then when you execute Bento use the environment variable to choose your resource: `FEATURE_REQUEST=get_foo bento -c ./your_config.yaml`.

### With Imports

However, Bento allows you to import resources from separate files with the cli flag `-r` or `-resources`, which can be a useful way to switch out resources with common names based on your chosen environment. For example, with a main configuration file `config.yaml`:

```yaml
pipeline:
  processors:
    - resource: get_foo
```

And then two resource files, one stored at the path `./staging/request.yaml`:

```yaml
processor_resources:
  - label: get_foo
    cache:
      resource: staging_cache
      operator: get
      key: '${! json("content") }'
```

And another stored at the path `./production/request.yaml`:

```yaml
processor_resources:
  - label: get_foo
    cache:
      resource: production_cache
      operator: get
      key: '${! json("content") }'
```

We can select our chosen resource by changing which file we import, either running:

```sh
bento -r ./staging/request.yaml -c ./config.yaml
```

Or:

```sh
bento -r ./production/request.yaml -c ./config.yaml
```

These flags also support wildcards, which allows you to import an entire directory of resource files like `bento -r "./staging/*.yaml" -c ./config.yaml`.
