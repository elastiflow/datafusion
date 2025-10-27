// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Execution plan for reading line-delimited Avro files

use std::any::Any;
use std::sync::Arc;

use arrow_avro::reader::{Reader, ReaderBuilder};
use arrow_avro::schema::AvroSchema;

use arrow::datatypes::SchemaRef;
use datafusion_common::error::Result;
use datafusion_common::{DataFusionError, HashSet, Statistics};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_stream::FileOpener;
use datafusion_datasource::schema_adapter::SchemaAdapterFactory;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;

use object_store::ObjectStore;
use serde_json::{from_str, to_string, Value};

/// Projects an Avro schema by retaining only the specified columns.
///
/// This function is designed to work with Arrow Avro to perform
/// column projection via a reader schema.
///
/// # Arguments
///
/// * `schema_str` - A string slice that holds the JSON representation of the Avro schema.
/// * `projection` - An `Option<&[String]>` containing the names of the columns to keep.
///                  If `None`, the original schema is returned.
///
/// # Returns
///
/// A `Result` containing the JSON string of the projected schema, or an `Error`
/// if parsing or processing fails.
pub fn project_schema(
    schema: AvroSchema,
    projection: Option<&[String]>,
) -> Result<AvroSchema> {
    let columns_to_keep = match projection {
        Some(p) => p,
        None => return Ok(schema),
    };
    let mut json_value: Value =
        from_str(&schema.json_string).map_err(|e| DataFusionError::External(e.into()))?;

    if let Value::Object(ref mut map) = json_value {
        if let Some(fields) = map.get_mut("fields") {
            if let Value::Array(ref mut fields_array) = fields {
                if columns_to_keep.is_empty() {
                    fields_array.clear();
                } else {
                    let projection_set: HashSet<&str> =
                        columns_to_keep.iter().map(AsRef::as_ref).collect();

                    fields_array.retain(|field| {
                        field
                            .get("name")
                            .and_then(Value::as_str)
                            .map_or(false, |name| projection_set.contains(name))
                    });
                }
            }
        }
    }

    let projected_json_string =
        to_string(&json_value).map_err(|e| DataFusionError::External(e.into()))?;

    Ok(AvroSchema {
        json_string: projected_json_string,
    })
}

/// AvroSource holds the extra configuration that is necessary for opening avro files
#[derive(Clone, Default)]
pub struct AvroSource {
    schema: Option<SchemaRef>,
    batch_size: Option<usize>,
    projection: Option<Vec<String>>,
    metrics: ExecutionPlanMetricsSet,
    projected_statistics: Option<Statistics>,
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
}

impl AvroSource {
    /// Initialize an AvroSource with default values
    pub fn new() -> Self {
        Self::default()
    }

    fn open<R: std::io::Read + std::io::BufRead>(&self, reader: R) -> Result<Reader<R>> {
        let avro_schema = AvroSchema::try_from(
            self.schema
                .as_ref()
                .expect("Schema must set before open")
                .as_ref(),
        )?;

        let projected_schema = project_schema(avro_schema, self.projection.as_deref())?;

        ReaderBuilder::new()
            .with_reader_schema(projected_schema)
            .with_batch_size(self.batch_size.expect("Batch size must set before open"))
            .build(reader)
            .map_err(Into::into)
    }
}

impl FileSource for AvroSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> Arc<dyn FileOpener> {
        Arc::new(private::AvroOpener {
            config: Arc::new(self.clone()),
            object_store,
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.batch_size = Some(batch_size);
        Arc::new(conf)
    }

    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.schema = Some(schema);
        Arc::new(conf)
    }
    fn with_projection(&self, config: &FileScanConfig) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.projection = config.projected_file_column_names();
        Arc::new(conf)
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.projected_statistics = Some(statistics);
        Arc::new(conf)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn statistics(&self) -> Result<Statistics> {
        let statistics = &self.projected_statistics;
        Ok(statistics
            .clone()
            .expect("projected_statistics must be set"))
    }

    fn file_type(&self) -> &str {
        "avro"
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _repartition_file_min_size: usize,
        _output_ordering: Option<LexOrdering>,
        _config: &FileScanConfig,
    ) -> Result<Option<FileScanConfig>> {
        Ok(None)
    }

    fn with_schema_adapter_factory(
        &self,
        schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Result<Arc<dyn FileSource>> {
        Ok(Arc::new(Self {
            schema_adapter_factory: Some(schema_adapter_factory),
            ..self.clone()
        }))
    }

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.clone()
    }
}

mod private {
    use super::*;
    use std::io::BufReader;

    use bytes::Buf;
    use datafusion_datasource::{file_stream::FileOpenFuture, PartitionedFile};
    use futures::StreamExt;
    use object_store::{GetResultPayload, ObjectStore};

    pub struct AvroOpener {
        pub config: Arc<AvroSource>,
        pub object_store: Arc<dyn ObjectStore>,
    }

    impl FileOpener for AvroOpener {
        fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
            let config = Arc::clone(&self.config);
            let object_store = Arc::clone(&self.object_store);
            Ok(Box::pin(async move {
                let r = object_store
                    .get(&partitioned_file.object_meta.location)
                    .await?;
                match r.payload {
                    GetResultPayload::File(file, _) => {
                        let buffered_reader = BufReader::new(file);
                        let reader = config.open(buffered_reader)?;
                        Ok(futures::stream::iter(reader)
                            .map(|r| r.map_err(Into::into))
                            .boxed())
                    }
                    GetResultPayload::Stream(_) => {
                        let bytes = r.bytes().await?;
                        let reader = config.open(bytes.reader())?;
                        Ok(futures::stream::iter(reader)
                            .map(|r| r.map_err(Into::into))
                            .boxed())
                    }
                }
            }))
        }
    }
}
