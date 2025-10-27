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

use arrow_avro::compression::CompressionCodec;
use arrow_avro::writer::format::AvroOcfFormat;
use arrow_avro::writer::WriterBuilder;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common_runtime::JoinSet;
use datafusion_datasource::ListingTableUrl;
use datafusion_execution::TaskContext;
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use futures::StreamExt;
use object_store::path::Path;
use std::sync::Arc;

/// Executes a query and writes the results to a partitioned Avro file.
pub async fn plan_to_avro(
    task_ctx: Arc<TaskContext>,
    plan: Arc<dyn ExecutionPlan>,
    path: impl AsRef<str>,
    compression: Option<CompressionTypeVariant>,
) -> datafusion_common::Result<()> {
    let path = path.as_ref();
    let parsed = ListingTableUrl::parse(path)?;
    let object_store_url = parsed.object_store();
    let store = task_ctx.runtime_env().object_store(&object_store_url)?;
    let mut join_set: JoinSet<datafusion_common::Result<()>> = JoinSet::new();

    // Convert compression variant to Avro compression codec
    let codec = match compression {
        Some(CompressionTypeVariant::UNCOMPRESSED) => None,
        Some(CompressionTypeVariant::BZIP2) => Some(CompressionCodec::Bzip2),
        Some(CompressionTypeVariant::XZ) => Some(CompressionCodec::Xz),
        Some(CompressionTypeVariant::ZSTD) => Some(CompressionCodec::ZStandard),
        Some(CompressionTypeVariant::GZIP) => Some(CompressionCodec::Deflate),
        None => None,
    };

    for i in 0..plan.output_partitioning().partition_count() {
        let plan: Arc<dyn ExecutionPlan> = Arc::clone(&plan);
        let filename = format!("{}/part-{i}.avro", parsed.prefix());
        let file = Path::parse(filename)?;
        let codec_clone = codec;

        let storeref = Arc::clone(&store);
        let mut stream = plan.execute(i, Arc::clone(&task_ctx))?;
        join_set.spawn(async move {
            let mut avro_writer: Option<
                arrow_avro::writer::Writer<Vec<u8>, AvroOcfFormat>,
            > = None;

            while let Some(next_batch) = stream.next().await {
                let batch = next_batch?;

                // Initialize the Avro writer on the first batch
                if avro_writer.is_none() {
                    let buffer = Vec::<u8>::new();
                    let writer = WriterBuilder::new(batch.schema().as_ref().clone())
                        .with_compression(codec_clone)
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

                // Write the complete Avro data to the object store
                storeref.put(&file, avro_data.into()).await?;
            }

            Ok(())
        });
    }

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(res) => res?,
            Err(e) => {
                if e.is_panic() {
                    std::panic::resume_unwind(e.into_panic());
                } else {
                    unreachable!();
                }
            }
        }
    }

    Ok(())
}
