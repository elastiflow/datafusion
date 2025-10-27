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

//! Apache Avro [`FileFormat`] abstractions

use crate::source::AvroSource;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::io::{BufReader, Read};
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow_avro::compression::CompressionCodec;
use arrow_avro::reader::ReaderBuilder;
use arrow_avro::writer::format::AvroOcfFormat;
use arrow_avro::writer::{WriterBuilder};
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::GetExt;
use datafusion_common::DEFAULT_AVRO_EXTENSION;
use datafusion_common::{internal_err, DataFusionError};
use datafusion_common::{Result, Statistics};
use datafusion_common_runtime::{JoinSet, SpawnedTask};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::source::DataSourceExec;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion_session::Session;

use async_trait::async_trait;
use bytes::Buf;
use datafusion_common::file_options::avro_writer::AvroWriterOptions;
use datafusion_datasource::display::FileGroupDisplay;
use datafusion_datasource::file_sink_config::{FileSink, FileSinkConfig};
use datafusion_datasource::sink::{DataSink, DataSinkExec};
use datafusion_datasource::write::demux::DemuxedStreamReceiver;
use datafusion_datasource::write::ObjectWriterBuilder;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::metrics::MetricsSet;
use object_store::{GetResultPayload, ObjectMeta, ObjectStore};
use tokio::io::AsyncWriteExt;
use datafusion_physical_expr_common::sort_expr::LexRequirement;

pub fn read_avro_schema_from_reader<R: Read>(reader: R) -> Result<SchemaRef> {
    let buffered_reader = BufReader::new(reader);

    let avro_reader = ReaderBuilder::new().build(buffered_reader)?;

    Ok(avro_reader.schema())
}

#[derive(Default)]
/// Factory struct used to create [`AvroFormat`]
pub struct AvroFormatFactory;

impl AvroFormatFactory {
    /// Creates an instance of [`AvroFormatFactory`]
    pub fn new() -> Self {
        Self {}
    }
}

impl FileFormatFactory for AvroFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        _format_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(AvroFormat))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(AvroFormat)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Debug for AvroFormatFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AvroFormatFactory").finish()
    }
}

impl GetExt for AvroFormatFactory {
    fn get_ext(&self) -> String {
        // Removes the dot, i.e. ".avro" -> "avro"
        DEFAULT_AVRO_EXTENSION[1..].to_string()
    }
}

/// Avro [`FileFormat`] implementation.
#[derive(Default, Debug)]
pub struct AvroFormat;

pub fn variant_to_codec(
    variant: CompressionTypeVariant,
) -> Result<Option<CompressionCodec>> {
    match variant {
        CompressionTypeVariant::UNCOMPRESSED => Ok(None),
        CompressionTypeVariant::BZIP2 => Ok(Some(CompressionCodec::Bzip2)),
        CompressionTypeVariant::XZ => Ok(Some(CompressionCodec::Xz)),
        CompressionTypeVariant::ZSTD => Ok(Some(CompressionCodec::ZStandard)),
        CompressionTypeVariant::GZIP => Ok(Some(CompressionCodec::Deflate)),
    }
}

#[async_trait]
impl FileFormat for AvroFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        AvroFormatFactory::new().get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        let ext = self.get_ext();
        match file_compression_type.get_variant() {
            CompressionTypeVariant::UNCOMPRESSED => Ok(ext),
            _ => internal_err!("Avro FileFormat does not support compression."),
        }
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let mut schemas = vec![];
        for object in objects {
            let r = store.as_ref().get(&object.location).await?;
            let schema = match r.payload {
                GetResultPayload::File(file, _) => read_avro_schema_from_reader(file)?,
                GetResultPayload::Stream(_) => {
                    let data = r.bytes().await?;
                    read_avro_schema_from_reader(data.reader())?
                }
            };
            schemas.push(schema);
        }
        let merged_schema =
            Schema::try_merge(schemas.iter().map(|s| s.as_ref().clone()))?;
        Ok(Arc::new(merged_schema))
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&table_schema))
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let config = FileScanConfigBuilder::from(conf)
            .with_source(self.file_source())
            .build();
        Ok(DataSourceExec::from_data_source(config))
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        conf: FileSinkConfig,
        _order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // figure out if writer gets instantiated with compression or not, if so, pass with_compression() here
        let writer_builder = WriterBuilder::new(input.schema().as_ref().clone());

        // TODO: Pass compression once it's figured out
        let avro_options = AvroWriterOptions::new(writer_builder, CompressionTypeVariant::UNCOMPRESSED);

        let avro_sink = AvroSink::new(
            conf,
            avro_options,
        );

        let sink_exec = DataSinkExec::new(
            input,
            Arc::new(avro_sink),
            _order_requirements
        );

        Ok(Arc::new(sink_exec))
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        Arc::new(AvroSource::new())
    }
}

/// Implements [`DataSink`] for writing to an Avro file.
pub struct AvroSink {
    /// Config options for writing data
    config: FileSinkConfig,
    writer_options: AvroWriterOptions,
}

impl Debug for AvroSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AvroSink").finish()
    }
}

impl DisplayAs for AvroSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "AvroSink(file_groups=",)?;
                FileGroupDisplay(&self.config.file_group).fmt_as(t, f)?;
                write!(f, ")")
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: avro")?;
                write!(f, "file={}", &self.config.original_url)
            }
        }
    }
}

impl AvroSink {
    pub fn new(config: FileSinkConfig, writer_options: AvroWriterOptions) -> Self {
        Self {
            config,
            writer_options,
        }
    }

    /// Retrieve the writer options
    pub fn writer_options(&self) -> &AvroWriterOptions {
        &self.writer_options
    }
}

#[async_trait]
impl FileSink for AvroSink {
    fn config(&self) -> &FileSinkConfig {
        &self.config
    }

    async fn spawn_writer_tasks_and_join(
        &self,
        context: &Arc<TaskContext>,
        demux_task: SpawnedTask<Result<()>>,
        mut file_stream_rx: DemuxedStreamReceiver,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<u64> {
        let mut file_write_tasks: JoinSet<std::result::Result<usize, DataFusionError>> =
            JoinSet::new();

        while let Some((path, mut rx)) = file_stream_rx.recv().await {
            let writer_options = self.writer_options.clone();
            let mut object_store_writer = ObjectWriterBuilder::new(
                FileCompressionType::UNCOMPRESSED,
                &path,
                Arc::clone(&object_store),
            )
            .with_buffer_size(Some(
                context
                    .session_config()
                    .options()
                    .execution
                    .objectstore_writer_buffer_size,
            ))
            .build()?;

            file_write_tasks.spawn(async move {
                let mut row_count = 0;
                let mut avro_writer: Option<
                    arrow_avro::writer::Writer<Vec<u8>, AvroOcfFormat>,
                > = None;

                while let Some(batch) = rx.recv().await {
                    row_count += batch.num_rows();

                    // Initialize the Avro writer on the first batch
                    if avro_writer.is_none() {
                        let codec = variant_to_codec(writer_options.compression)?;
                        let buffer = Vec::<u8>::new();
                        let writer = WriterBuilder::new(batch.schema().as_ref().clone())
                            .with_compression(codec)
                            .build::<_, AvroOcfFormat>(buffer)?;
                        avro_writer = Some(writer);
                    }

                    // Write the batch to the Avro writer
                    if let Some(ref mut writer) = avro_writer {
                        writer.write(&batch)?;
                    }
                }

                // Finish the Avro writer and write to object store
                if let Some(mut writer) = avro_writer {
                    writer.finish()?;
                    let avro_data = writer.into_inner();
                    object_store_writer.write_all(&avro_data).await?;
                    object_store_writer.shutdown().await?;
                }

                Ok(row_count)
            });
        }

        let mut total_row_count = 0;
        while let Some(result) = file_write_tasks.join_next().await {
            match result {
                Ok(r) => {
                    total_row_count += r?;
                }
                Err(e) => {
                    if e.is_panic() {
                        std::panic::resume_unwind(e.into_panic());
                    } else {
                        unreachable!();
                    }
                }
            }
        }

        demux_task
            .join_unwind()
            .await
            .map_err(|e| DataFusionError::ExecutionJoin(Box::new(e)))??;

        Ok(total_row_count as u64)
    }
}

#[async_trait]
impl DataSink for AvroSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn schema(&self) -> &SchemaRef {
        self.config.output_schema()
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        FileSink::write_all(self, data, context).await
    }
}
