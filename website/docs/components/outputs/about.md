---
title: Outputs
sidebar_label: About
---

An output is a sink where we wish to send our consumed data after applying an optional array of [processors][processors]. Only one output is configured at the root of a Bento config. However, the output can be a [broker][output.broker] which combines multiple outputs under a chosen brokering pattern, or a [switch][output.switch] which is used to multiplex against different outputs.

An output config section looks like this:

```yaml
output:
  label: my_s3_output

  aws_s3:
    bucket: TODO
    path: '${! metadata("kafka_topic") }/${! json("message.id") }.json'

  # Optional list of processing steps
  processors:
    - mapping: '{"message":this,"meta":{"link_count":this.links.length()}}'
```

## Back Pressure

Bento outputs apply back pressure to components upstream. This means if your output target starts blocking traffic Bento will gracefully stop consuming until the issue is resolved.

## Retries

When a Bento output fails to send a message the error is propagated back up to the input, where depending on the protocol it will either be pushed back to the source as a Noack (e.g. AMQP) or will be reattempted indefinitely with the commit withheld until success (e.g. Kafka).

It's possible to instead have Bento indefinitely retry an output until success with a [`retry`][output.retry] output. Some other outputs, such as the [`broker`][output.broker], might also retry indefinitely depending on their configuration.

## Dead Letter Queues

It's possible to create fallback outputs for when an output target fails using a [`fallback`][output.fallback] output:

```yaml
output:
  fallback:
    - aws_s3:
        bucket: TODO
        path: '${! json("message.id") }.json'
        max_in_flight: 20

    - file:
        path: /var/lib/bento/dlq.jsonl
```

## Multiplexing Outputs

There are a few different ways of multiplexing in Bento, here's a quick run through:

### Interpolation Multiplexing

Some output fields support [field interpolation][interpolation], which is a super easy way to multiplex messages based on their contents in situations where you are multiplexing to the same service.

For example, multiplexing against S3 key prefixes is a common pattern:

```yaml
output:
  aws_s3:
    bucket: TODO
    path: ${! metadata("target_prefix") }/${! uuid_v4() }.json
```

Refer to the field documentation for a given output to see if it support interpolation.

### Switch Multiplexing

A more advanced form of multiplexing is to route messages to different output configurations based on a query. This is easy with the [`switch` output][output.switch]:

```yaml
output:
  switch:
    cases:
      - check: this.type == "foo"
        output:
          file:
            path: /var/lib/bento/the_foos.jsonl

      - check: this.type == "bar"
        output:
          aws_s3:
            bucket: dealing_with_mike
            path: mikes_bars/${! uuid_v4() }.json

      - output:
          stdout: {}
          processors:
            - mapping: |
                root = this
                root.type = this.type.not_null() | "unknown"
```

## Labels

Outputs have an optional field `label` that can uniquely identify them in observability data such as metrics and logs. This can be useful when running configs with multiple outputs, otherwise their metrics labels will be generated based on their composition. For more information check out the [metrics documentation][metrics.about].

import ComponentsByCategory from '@theme/ComponentsByCategory';

## Categories

<ComponentsByCategory type="outputs"></ComponentsByCategory>

import ComponentSelect from '@theme/ComponentSelect';

<ComponentSelect type="outputs"></ComponentSelect>

[processors]: /docs/components/processors/about
[output.broker]: /docs/components/outputs/broker
[output.switch]: /docs/components/outputs/switch
[output.retry]: /docs/components/outputs/retry
[output.fallback]: /docs/components/outputs/fallback
[interpolation]: /docs/configuration/interpolation
[metrics.about]: /docs/components/metrics/about