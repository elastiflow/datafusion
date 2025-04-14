use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::io::BufReader;
use std::sync::Arc;
use arrow::array::RecordBatch;
use arrow::avro;
use arrow::avro::ReaderBuilder;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::ArrowError;
use object_store::{GetResultPayload, ObjectMeta, ObjectStore};
use datafusion_common::{internal_err, GetExt, Statistics, DEFAULT_AVRO_EXTENSION, Result};
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_datasource::decoder::Decoder;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::source::DataSourceExec;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_session::Session;

use crate::source::AvroSource;
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

impl fmt::Debug for AvroFormatFactory {
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
    // Error: Lifetimes do not match method in trait
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
                GetResultPayload::File(mut file, _) => {
                    // read schema from file
                    let reader = ReaderBuilder::new()
                        .build(BufReader::new(&mut file))?;
                    Ok(reader.schema())
                }
                GetResultPayload::Stream(s) => {
                    // Need to get a ReaderBuilder instance from the stream and use that to get the schema

                    // This is failing because the BufReader::new(s) is invalid for build_decoder
                    let reader = ReaderBuilder::new()
                        .build_decoder(BufReader::new(s))?;
                    Ok(reader.schema())
                }
            };
            schemas.push(schema?);
        }
        let merged_schema = Schema::try_merge(schemas.clone())?;
        Ok(Arc::new(merged_schema))
    }
    // Error: Lifetimes do not match method in trait
    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&table_schema))
    }
    // Error: Lifetimes do not match method in trait
    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        conf: FileScanConfig,
        _filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let config = FileScanConfigBuilder::from(conf)
            .with_source(self.file_source())
            .build();
        Ok(DataSourceExec::from_data_source(config))
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        Arc::new(AvroSource::new())
    }
}

#[derive(Debug)]
pub struct AvroDecoder {
    inner: avro::reader::Decoder,
}

impl AvroDecoder {
    pub fn new(decoder: avro::reader::Decoder) -> Self {
        Self { inner: decoder }
    }
}

impl Decoder for AvroDecoder {
    fn decode(&mut self, buf: &[u8]) -> Result<usize, ArrowError> {
        self.inner.decode(buf)
    }

    fn flush(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        self.inner.flush()
    }

    fn can_flush_early(&self) -> bool {
        false
    }
}