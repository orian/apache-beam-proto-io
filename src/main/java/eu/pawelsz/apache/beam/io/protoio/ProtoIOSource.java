package eu.pawelsz.apache.beam.io.protoio;

import com.google.common.io.CountingInputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.NoSuchElementException;

public class ProtoIOSource<T extends Message> extends FileBasedSource<T> implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(ProtoIOSource.class);

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
        LOG.error("source for subrange for subrange: " + start + " " + end);
        return new ProtoIOSource<T>(protoMessageClass, fileMetadata, DEFAULT_MIN_BUNDLE_SIZE, start, end);
    }

    @Override
    protected FileBasedReader<T> createSingleFileReader(PipelineOptions pipelineOptions) {
        return new ProtoReader(this, protoMessageClass);
    }

    @Override
    protected boolean isSplittable() throws Exception {
//        LOG.info("isSplittable");
        return getMode() == Mode.FILEPATTERN;
    }

    @Override
    public Coder<T> getDefaultOutputCoder() {
        return ProtoCoder.of(protoMessageClass);
    }

    private interface Wrapper<T> {
        T parse(InputStream inputStream) throws InvalidProtocolBufferException;
    }

    private static class ThrowingWrapper<T> implements Wrapper<T> {
        private final Parser<T> parser_;

        public ThrowingWrapper(Parser<T> p) {
            this.parser_ = p;
        }

        public static <T> ThrowingWrapper<T> of(Parser<T> parser) {
            return new ThrowingWrapper<T>(parser);
        }

        @Override
        public T parse(InputStream inputStream) throws InvalidProtocolBufferException {
            return (T) this.parser_.parseDelimitedFrom(inputStream);
        }
    }

    private static class ExceptionCatchingWrapper<T> implements Wrapper<T> {
        private final Parser<T> parser_;

        public ExceptionCatchingWrapper(Parser<T> p) {
            this.parser_ = p;
        }

        public static <T> ThrowingWrapper<T> of(Parser<T> parser) {
            return new ThrowingWrapper<T>(parser);
        }

        @Override
        public T parse(InputStream inputStream) throws InvalidProtocolBufferException {
            return (T) this.parser_.parseDelimitedFrom(inputStream);
        }
    }

    static class ProtoReader<T extends Message> extends ProtoIOSource.FileBasedReader<T> {
        private final Class<T> protoMessageClass;
        private T current;
        private CountingInputStream inputStream;
        private long currentOffset = 0;
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
            this.inputStream = new CountingInputStream(Channels.newInputStream(channel));
        }

        @Override
        protected boolean readNextRecord() throws IOException {
            currentOffset = inputStream.getCount();
            this.readNum++;
            try {
                current = (T) getParser().parseDelimitedFrom(inputStream);
            } catch (InvalidProtocolBufferException e) {
                System.err.printf("at msg: %d (offset: %d)", this.readNum, this.currentOffset);
            }
            return (current != null);
        }

        @Override
        protected long getCurrentOffset() throws NoSuchElementException {
            // TODO find a way to get a real consumed bytes.
            return currentOffset;
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
            return false;
        }
    }
}
