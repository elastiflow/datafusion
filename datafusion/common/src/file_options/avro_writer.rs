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

//! Options related to how avro files should be written

use crate::parsers::CompressionTypeVariant;

#[cfg(feature = "avro")]
use arrow_avro::writer::WriterBuilder;

#[derive(Clone, Debug)]
pub struct AvroWriterOptions {
    #[cfg(feature = "avro")]
    pub writer_options: WriterBuilder,
    pub compression: CompressionTypeVariant,
}

#[cfg(feature = "avro")]
impl AvroWriterOptions {
    pub fn new(
        writer_options: WriterBuilder,
        compression: CompressionTypeVariant,
    ) -> Self {
        Self {
            writer_options,
            compression,
        }
    }
}
