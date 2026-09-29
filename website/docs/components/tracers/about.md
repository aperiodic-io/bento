---
title: Tracers
sidebar_label: About
---

A tracer type represents a destination for Bento to send tracing events to. The only tracer included in this build is `none`, which disables tracing.

When a tracer is configured all messages will be allocated a root span during ingestion that represents their journey through a Bento pipeline. Many Bento processors create spans, and so tracing is a great way to analyse the pathways of individual messages as they progress through a Bento instance.

Some inputs, such as `kafka_franz`, can be configured to extract a root span by using the `extract_tracing_map` field.

A tracer config section looks like this:

```yaml
tracer:
  none: {}
```

WARNING: Although the configuration spec of this component is stable the format of spans, tags and logs created by Bento is subject to change as it is tuned for improvement.

import ComponentSelect from '@theme/ComponentSelect';

<ComponentSelect type="tracers" singular="tracing target"></ComponentSelect>

