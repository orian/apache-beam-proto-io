package eu.pawelsz.apache.beam.io.protoio;

import com.google.common.io.CountingInputStream;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.NoSuchElementException;

/**
 * Created by orian on 26.05.17.
 */
public class ProtoIOSource<T extends Message> extends FileBasedSource<T> implements Serializable {

    private final Class<T> protoMessageClass;
    private static final int DEFAULT_MIN_BUNDLE_SIZE = 1024;

    public static <T extends Message> ProtoIOSource<T> from(Class<T> recordClass, String fileOrPatternSpec) {
        return new ProtoIOSource<T>(recordClass, ValueProvider.StaticValueProvider.of(fileOrPatternSpec),
                DEFAULT_MIN_BUNDLE_SIZE);
    }

    public static <T extends Message> ProtoIOSource<T> from(Class<T> recordClass, ValueProvider<String> fileOrPatternSpec) {
        return new ProtoIOSource<T>(recordClass, fileOrPatternSpec, DEFAULT_MIN_BUNDLE_SIZE);
    }

    private ProtoIOSource(Class<T> recordClass, ValueProvider<String> fileOrPatternSpec) {
        super(fileOrPatternSpec, 1L);
        this.protoMessageClass = recordClass;
    }

    private ProtoIOSource(Class<T> recordClass, ValueProvider<String> fileOrPatternSpec, long minBundleSize) {
        super(fileOrPatternSpec, minBundleSize);
        this.protoMessageClass = recordClass;
    }

    private ProtoIOSource(Class<T> recordClass, MatchResult.Metadata fileMetadata, long minBundleSize,
                          long startOffset, long endOffset) {
        super(fileMetadata, minBundleSize, startOffset, endOffset);
        this.protoMessageClass = recordClass;
    }

    @Override
    protected FileBasedSource<T> createForSubrangeOfFile(MatchResult.Metadata fileMetadata, long start, long end) {
        return new ProtoIOSource<T>(protoMessageClass, fileMetadata, DEFAULT_MIN_BUNDLE_SIZE, start, end);
    }

    @Override
    protected FileBasedReader<T> createSingleFileReader(PipelineOptions pipelineOptions) {
        return new ProtoReader(this, protoMessageClass);
    }

    @Override
    protected boolean isSplittable() throws Exception {
        return false;
    }

    @Override
    public Coder<T> getDefaultOutputCoder() {
        return ProtoCoder.of(protoMessageClass);
    }

    static class ProtoReader<T extends Message> extends ProtoIOSource.FileBasedReader<T> {
        private final Class<T> protoMessageClass;
        private T current;
        private ReadableByteChannel channel;
        private CountingInputStream inputStream;
        private long currentOffset = 0;
        private boolean realOffset = false;
        private long readNum = 0;

        public ProtoReader(ProtoIOSource<T> source, Class<T> protoMessageType) {
            super(source);
            this.protoMessageClass = protoMessageType;
        }

        private Parser<T> memoizedParser;

        /**
         * Get the memoized {@link Parser}, possibly initializing it lazily.
         */
        private Parser<T> getParser() {
            if (memoizedParser == null) {
                try {
                    @SuppressWarnings("unchecked")
                    T protoMessageInstance = (T) protoMessageClass.getMethod("getDefaultInstance").invoke(null);
                    @SuppressWarnings("unchecked")
                    Parser<T> tParser = (Parser<T>) protoMessageInstance.getParserForType();
                    memoizedParser = tParser;
                } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                    throw new IllegalArgumentException(e.getMessage());
                }
            }
            return memoizedParser;
        }

        @Override
        protected void startReading(ReadableByteChannel channel) throws IOException {
            this.channel = channel;
            this.inputStream = new CountingInputStream(Channels.newInputStream(channel));
        }

        @Override
        protected boolean readNextRecord() throws IOException {
            currentOffset = inputStream.getCount();
            this.readNum++;
            current = (T) getParser().parseDelimitedFrom(inputStream);
            return (current != null);
        }

        @Override
        protected long getCurrentOffset() throws NoSuchElementException {
            // TODO find a way to get a real consumed bytes.
            return this.realOffset ? currentOffset : this.readNum;
        }

        @Override
        public T getCurrent() throws NoSuchElementException {
            if (current == null) {
                throw new NoSuchElementException();
            }
            return current;
        }

        @Override
        protected boolean isAtSplitPoint() {
            // Every record is at a split point.
            return true;
        }

        @Override
        public boolean allowsDynamicSplitting() {
            return true;
        }
    }
}
