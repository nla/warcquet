/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2022 National Library of Australia
 */

package org.netpreserve.warcquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;

public class WarcquetWriterBuilder extends ParquetWriter.Builder<CaptureEvent, WarcquetWriterBuilder> {

    protected WarcquetWriterBuilder(OutputFile path) {
        super(path);
    }

    @Override
    protected WarcquetWriterBuilder self() {
        return this;
    }

    @Override
    protected WriteSupport<CaptureEvent> getWriteSupport(Configuration conf) {
        return new WarcquetWriteSupport();
    }
}
