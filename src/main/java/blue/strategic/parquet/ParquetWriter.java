package blue.strategic.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.DelegatingPositionOutputStream;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;

public final class ParquetWriter<T> implements Closeable {

    private final org.apache.parquet.hadoop.ParquetWriter<T> writer;

    public static <T> ParquetWriter.Builder<T> builder(File out, MessageType schema, Dehydrator<T> dehydrator) {
        return new Builder<T>(new OutputFileImpl(out), schema, dehydrator)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0);
    }

    // Kept for backwards compatibility
    public static <T> ParquetWriter<T> writeFile(MessageType schema, File out, Dehydrator<T> dehydrator) throws IOException {
        return builder(out, schema, dehydrator).buildWriter();
    }

    private ParquetWriter(org.apache.parquet.hadoop.ParquetWriter<T> writer) {
        this.writer = writer;
    }

    public void write(T record) throws IOException {
        writer.write(record);
    }

    @Override
    public void close() throws IOException {
        this.writer.close();
    }

    public static final class Builder<T> extends org.apache.parquet.hadoop.ParquetWriter.Builder<T, ParquetWriter.Builder<T>> {
        private MessageType schema;
        private Dehydrator<T> dehydrator;

        public Builder(File file) {
            super(new OutputFileImpl(file));
        }

        public Builder(OutputFile of) {
            super(of);
        }

        public Builder(File file, MessageType schema, Dehydrator<T> dehydrator) {
            this(new OutputFileImpl(file), schema, dehydrator);
        }

        public Builder(OutputFile file, MessageType schema, Dehydrator<T> dehydrator) {
            super(file);
            this.schema = schema;
            this.dehydrator = dehydrator;
        }

        public ParquetWriter.Builder<T> withType(MessageType schema) {
            this.schema = schema;
            return this;
        }

        public ParquetWriter.Builder<T> withDehydrator(Dehydrator<T> dehydrator) {
            this.dehydrator = dehydrator;
            return this;
        }

        public ParquetWriter<T> buildWriter() throws IOException {
            return new ParquetWriter<>(super.build());
        }

        @Override
        protected ParquetWriter.Builder<T> self() {
            return this;
        }

        @Override
        protected WriteSupport<T> getWriteSupport(Configuration conf) {
            return new SimpleWriteSupport<>(schema, dehydrator);
        }

    }

    private static class OutputFileImpl implements OutputFile {
        private final File out;

        public OutputFileImpl(File out) {
            this.out = out;
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) throws IOException {
            return createOrOverwrite(blockSizeHint);
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
            FileOutputStream fos = new FileOutputStream(out);
            return new DelegatingPositionOutputStream(fos) {
                @Override
                public long getPos() throws IOException {
                    return fos.getChannel().position();
                }
            };
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 1024L;
        }

        @Override
        public String toString() {
            return "OutputFile[" + out.getAbsolutePath() + "]";
        }
    }

    private static class SimpleWriteSupport<T> extends WriteSupport<T> {
        private final MessageType schema;
        private final Dehydrator<T> dehydrator;
        private final ValueWriter valueWriter = SimpleWriteSupport.this::writeField;

        private RecordConsumer recordConsumer;

        SimpleWriteSupport(MessageType schema, Dehydrator<T> dehydrator) {
            this.schema = schema;
            this.dehydrator = dehydrator;
        }

        @Override
        public WriteContext init(Configuration configuration) {
            return new WriteContext(schema, Collections.emptyMap());
        }

        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
            this.recordConsumer = recordConsumer;
        }

        @Override
        public void write(T record) {
            recordConsumer.startMessage();
            dehydrator.dehydrate(record, valueWriter);
            recordConsumer.endMessage();
        }

        @Override
        public String getName() {
            return "blue.strategic.parquet.ParquetWriter";
        }

        private void writeField(String name, Object value) {
            int fieldIndex = schema.getFieldIndex(name);
            PrimitiveType type = schema.getType(fieldIndex).asPrimitiveType();
            recordConsumer.startField(name, fieldIndex);

            switch (type.getPrimitiveTypeName()) {
            case INT32: recordConsumer.addInteger((int)value); break;
            case INT64: recordConsumer.addLong((long)value); break;
            case DOUBLE: recordConsumer.addDouble((double)value); break;
            case BOOLEAN: recordConsumer.addBoolean((boolean)value); break;
            case FLOAT: recordConsumer.addFloat((float)value); break;
            case BINARY:
                if (type.getLogicalTypeAnnotation() == LogicalTypeAnnotation.stringType()) {
                    recordConsumer.addBinary(Binary.fromString((String)value));
                } else {
                    throw new UnsupportedOperationException("We don't support writing " + type.getLogicalTypeAnnotation());
                }
                break;
            default:
                throw new UnsupportedOperationException("We don't support writing " + type.getPrimitiveTypeName());
            }
            recordConsumer.endField(name, fieldIndex);
        }
    }
}
