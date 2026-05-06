<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache DataFusion Parquet DataSource

[Apache DataFusion] is an extensible query execution framework, written in Rust, that uses [Apache Arrow] as its in-memory format.

This crate is a submodule of DataFusion that defines an [Apache Parquet] based file source.

Most projects should use the [`datafusion`] crate directly, which re-exports
this module. If you are already using the [`datafusion`] crate, there is no
reason to use this crate directly in your project as well.

## Opt-in deep projection

A Parquet leaf is one stored column inside a nested field.
`PushAllProjectionHints` collects the leaves that a complete physical plan needs.
It can skip unused struct fields inside lists and map values, including fields
accessed above windows, joins, and computed field projections.
Register the rule after the default physical optimizer rules:

```rust,ignore
use std::sync::Arc;
use datafusion::execution::SessionStateBuilder;
use datafusion_datasource_parquet::push_all_projection_hints::PushAllProjectionHints;

let state = SessionStateBuilder::new()
    .with_default_features()
    .with_physical_optimizer_rule(Arc::new(PushAllProjectionHints {}))
    .build();
```

The rule preserves output types, null values, and fields that expressions consume.
The scan fills unused fields with nulls or default values after decoding.
Whole-column references retain every field.
Maps retain all keys and both entry fields while pruning unused value fields.
List, large-list, fixed-size-list, and list-view paths preserve their container
shape. Unknown operators retain complete reads. Final aggregate stages retain
their accumulator inputs.
The existing schema-driven projection remains active without this rule.

Binary and JSON physical-plan serialization preserve the hints. Plans without
hints retain their existing behavior. Invalid hint indices and missing hint
expressions fail during decoding.

Do not reuse a hinted scan with new consumers unless you rerun the optimizer.
Its hints describe the complete optimized plan, not an independent scan.

[apache arrow]: https://arrow.apache.org/
[apache datafusion]: https://datafusion.apache.org/
[apache parquet]: https://parquet.apache.org/
[`datafusion`]: https://crates.io/crates/datafusion
