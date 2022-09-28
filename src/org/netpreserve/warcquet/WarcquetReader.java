package org.netpreserve.warcquet;

import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.netpreserve.warcquet.util.PathInputFile;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class WarcquetReader implements Closeable, Iterable<CaptureEvent> {
    private final ParquetFileReader parquetReader;
    private final MessageColumnIO columnIO;
    private final CaptureEventMaterializer materializer;
    private PageReadStore rowGroup;
    private int row = Integer.MAX_VALUE;
    private RecordReader<CaptureEvent> recordReader;
    private boolean exhausted;

    WarcquetReader(Path file) throws IOException {
        this.parquetReader = ParquetFileReader.open(new PathInputFile(file));
        var schema = parquetReader.getFileMetaData().getSchema();
        this.columnIO = new ColumnIOFactory().getColumnIO(schema);
        this.materializer = new CaptureEventMaterializer();
    }

    @Override
    public void close() throws IOException {
        parquetReader.close();
    }

    @Override
    public Iterator<CaptureEvent> iterator() {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                if (exhausted) return false;
                while (rowGroup == null || row >= rowGroup.getRowCount()) {
                    try {
                        rowGroup = parquetReader.readNextRowGroup();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    if (rowGroup == null) {
                        exhausted = true;
                        return false;
                    }
                    row = 0;
                    recordReader = columnIO.getRecordReader(rowGroup, materializer);
                }
                return true;
            }

            @Override
            public CaptureEvent next() {
                if (!hasNext()) throw new NoSuchElementException();
                row++;
                return recordReader.read();
            }
        };
    }
}
