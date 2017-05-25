package eu.pawelsz.apache.beam.io.protoio;

import com.google.common.io.CountingInputStream;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.NoSuchElementException;

public class ProtoIO {
    private static final Logger LOG = LoggerFactory.getLogger(ProtoIO.class);

    public static <T extends Message> Source<T> source (Class<T> recordClass, String fileOrPatternSpec) {
        return Source.from(recordClass, fileOrPatternSpec);
    }

    public static <T extends Message> Source<T> source (Class<T> recordClass, ValueProvider<String> fileOrPatternSpec) {
        return Source.from(recordClass, fileOrPatternSpec);
    }

    public static <T extends Message> Sink<T> sink(String baseOutputFilename) {
        ResourceId resourceId = FileBasedSink.convertToFileResourceIfPossible(baseOutputFilename);
        ValueProvider<ResourceId> valueProvider = ValueProvider.StaticValueProvider.of(resourceId);

        return new Sink<T>(valueProvider,
                DefaultFilenamePolicy.constructUsingStandardParameters(
                        valueProvider, null, null));
    }

    public static <T extends Message> Sink<T> sink(String baseOutputFilename, String extension,
                                                   FileBasedSink.WritableByteChannelFactory factory) {

        ResourceId resourceId = FileBasedSink.convertToFileResourceIfPossible(baseOutputFilename);
        ValueProvider<ResourceId> valueProvider = ValueProvider.StaticValueProvider.of(resourceId);

        return new Sink<T>(valueProvider,
                DefaultFilenamePolicy.constructUsingStandardParameters(
                        valueProvider, null, extension));
    }

    public static <T extends Message> Sink<T> sink(ValueProvider<String> baseOutputFilename,
                                                   String extension,
                                                   FileBasedSink.WritableByteChannelFactory factory) {
        ValueProvider<ResourceId> valueProvider = ValueProvider.NestedValueProvider.of(
                baseOutputFilename, FileBasedSink::convertToFileResourceIfPossible);

        return new Sink<T>(valueProvider,
                DefaultFilenamePolicy.constructUsingStandardParameters(
                        valueProvider, null, extension));
    }

    public static <T extends Message> PTransform<PCollection<T>, PDone> write(
            String baseOutputFilename) {
        return WriteFiles.to(sink(baseOutputFilename));
    }

    public static <T extends Message> PTransform<PCollection<T>, PDone> write(
            String baseOutputFilename, String extension,
            FileBasedSink.WritableByteChannelFactory factory) {
        return WriteFiles.to(sink(baseOutputFilename, extension, factory));
    }

    public static <T extends Message> PTransform<PCollection<T>, PDone> write(
            ValueProvider<String> baseOutputFilename, String extension,
            FileBasedSink.WritableByteChannelFactory factory) {
        return WriteFiles.to(sink(baseOutputFilename, extension, factory));
    }

    public static class FileDesc implements Serializable {
        private final String name;  // path to file
        private final long offset;  // where to start the read
        private final long len;     // where to finish, -1 for the EOF

        public FileDesc(String name, long offset, long len) {
            this.name = name;
            this.offset = offset;
            this.len = len;
        }
    }

    public static class Source<T extends Message> extends FileBasedSource<T> implements Serializable {

        private final Class<T> protoMessageClass;
        private static final int DEFAULT_MIN_BUNDLE_SIZE = 1024;

        public static <T extends Message> Source<T> from(Class<T> recordClass, String fileOrPatternSpec) {
            return new Source<T>(recordClass, ValueProvider.StaticValueProvider.of(fileOrPatternSpec),
                    DEFAULT_MIN_BUNDLE_SIZE);
        }

        public static <T extends Message> Source<T> from(Class<T> recordClass, ValueProvider<String> fileOrPatternSpec) {
            return new Source<T>(recordClass, fileOrPatternSpec, DEFAULT_MIN_BUNDLE_SIZE);
        }

        private Source(Class<T> recordClass, ValueProvider<String> fileOrPatternSpec) {
            super(fileOrPatternSpec, 1L);
            this.protoMessageClass = recordClass;
        }

        private Source(Class<T> recordClass, ValueProvider<String> fileOrPatternSpec, long minBundleSize) {
            super(fileOrPatternSpec, minBundleSize);
            this.protoMessageClass = recordClass;
        }

        private Source(Class<T> recordClass, MatchResult.Metadata fileMetadata, long minBundleSize,
                       long startOffset, long endOffset) {
            super(fileMetadata, minBundleSize, startOffset, endOffset);
            this.protoMessageClass = recordClass;
        }

        @Override
        protected FileBasedSource<T> createForSubrangeOfFile(MatchResult.Metadata fileMetadata, long start, long end) {
            return new Source<T>(protoMessageClass, fileMetadata, DEFAULT_MIN_BUNDLE_SIZE, start, end);
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
    }

    static class ProtoReader<T extends Message> extends Source.FileBasedReader<T> {
        private final Class<T> protoMessageClass;
        private T current;
        private ReadableByteChannel channel;
        private CountingInputStream inputStream;
        private long currentOffset = 0;
        private boolean realOffset = false;
        private long readNum = 0;

        public ProtoReader(Source<T> source, Class<T> protoMessageType) {
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

//    // @deprecated
//    public static class DeprecatedSource<T extends Message> extends BoundedSource<T> implements Serializable {
//        private final ImmutableList<String> filenames;
//        private final ImmutableList<FileDesc> files;
//        private final Class<T> protoMessageClass;
//
//        public DeprecatedSource(Class<T> protoMessageClass) {
//            this.protoMessageClass = protoMessageClass;
//            filenames = ImmutableList.copyOf(new LinkedList<String>());
//            files = ImmutableList.copyOf(new LinkedList<FileDesc>());
//        }
//
//        public DeprecatedSource(Class<T> protoMessageClass, Collection<String> filenames) {
//            this.protoMessageClass = protoMessageClass;
//            this.filenames = ImmutableList.copyOf(filenames);
//            ImmutableList.Builder<FileDesc> builder = ImmutableList.<FileDesc>builder();
//            for (String s : filenames) {
//                builder = builder.add(new FileDesc(s, 0, -1));
//            }
//            files = builder.build();
//        }
//
//        public DeprecatedSource(Class<T> protoMessageClass, ImmutableList<String> filenames,
//                                ImmutableList<FileDesc> files) {
//            this.protoMessageClass = protoMessageClass;
//            this.filenames = filenames;
//            this.files = files;
//        }
//
//        public DeprecatedSource<T> addFile(String filename) {
//            FileDesc fd = new FileDesc(filename, 0, -1);
//            return new DeprecatedSource<T>(protoMessageClass,
//                    ImmutableList.<String>builder().addAll(filenames).add(filename).build(),
//                    ImmutableList.<FileDesc>builder().addAll(files).add(fd).build());
//        }
//
//        public DeprecatedSource<T> addFiles(Collection<String> fnames) {
//            ImmutableList.Builder<FileDesc> builder = ImmutableList.<FileDesc>builder().addAll(files);
//            for (String s : fnames) {
//                builder = builder.add(new FileDesc(s, 0, -1));
//            }
//
//            return new DeprecatedSource<T>(protoMessageClass,
//                    ImmutableList.<String>builder().addAll(filenames).addAll(fnames).build(),
//                    builder.build());
//        }
//
//        @Override
//        public List<? extends BoundedSource<T>> splitIntoBundles(long desiredBundleSizeBytes,
//                                                                 PipelineOptions pipelineOptions) throws Exception {
//            LOG.debug("SOURCE split into bundles");
//            List<DeprecatedSource<T>> ret = new LinkedList<DeprecatedSource<T>>();
//            int buckets = (int) Math.floor((double) getEstimatedSizeBytes(pipelineOptions) / desiredBundleSizeBytes);
//            buckets = Math.max(Math.min(buckets, files.size()), 1);
//            int filesPerBucket = files.size() / buckets; // how many files per bucket
//            int extras = files.size() % buckets;
//            int idx = 0;
//            for (int i = 0; i < buckets; ++i) {
//                int filesInBucket = filesPerBucket;
//                if (extras > 0) {
//                    filesInBucket++;
//                    extras--;
//                }
//                ret.add(new DeprecatedSource<T>(protoMessageClass, filenames.subList(idx, idx + filesInBucket)));
//                idx += filesInBucket;
//            }
//            LOG.debug("SOURCE returns " + ret.size() + " bundles");
//            return ret;
//        }
//
//        @Override
//        public long getEstimatedSizeBytes(PipelineOptions pipelineOptions) throws Exception {
//            long sizeSum = 0;
//            for (FileDesc f : files) {
//                File f1 = new File(f.name);
//                if (!f1.canRead()) {
//                    throw new IOException("cannot open " + f.name);
//                }
//                sizeSum += f1.length();
//            }
//            LOG.info("SOURCE estimated size: " + sizeSum);
//            return sizeSum;
//        }
//
//        @Override
//        public BoundedSource.BoundedReader<T> createReader(PipelineOptions pipelineOptions) throws IOException {
//            return new DeprecatedReader(this, protoMessageClass);
//        }
//
//        public void validate() {
//            Preconditions.checkState(!filenames.isEmpty(), "empty");
//        }
//
//        @Override
//        public Coder<T> getDefaultOutputCoder() {
//            return ProtoCoder.of(protoMessageClass);
//        }
//    }
//
//    // @deprecated
//    public static class DeprecatedReader<T extends Message> extends BoundedSource.BoundedReader<T> {
//        private DeprecatedSource source;
//        private T current;
//        Class<T> protoMessageClass;
//        private File file;
//        private int fileIdx = -1;
//        private InputStream inputStream;
//        private FileChannel fileChannel;
//        private FileDesc fileDesc;
//        private long untilByte = 0;
//
//        private Parser<T> memoizedParser;
//
//        /**
//         * Get the memoized {@link Parser}, possibly initializing it lazily.
//         */
//        private Parser<T> getParser() {
//            if (memoizedParser == null) {
//                try {
//                    @SuppressWarnings("unchecked")
//                    T protoMessageInstance = (T) protoMessageClass.getMethod("getDefaultInstance").invoke(null);
//                    @SuppressWarnings("unchecked")
//                    Parser<T> tParser = (Parser<T>) protoMessageInstance.getParserForType();
//                    memoizedParser = tParser;
//                } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
//                    throw new IllegalArgumentException(e.getMessage());
//                }
//            }
//            return memoizedParser;
//        }
//
//        public DeprecatedReader(DeprecatedSource source, Class<T> protoMessageType) {
//            this.source = source;
//            this.protoMessageClass = protoMessageType;
//        }
//
//        @Override
//        public boolean start() throws IOException {
//            return openNextFile() && advance();
//        }
//
//        @Override
//        public boolean advance() throws IOException {
//            if (inputStream.available() == 0) {
////                System.out.println("at position: " + fileChannel.position());
//                LOG.debug("SOURCE input stream empty");
//                if (!openNextFile()) {
//                    return false;
//                }
//            }
//
//            current = (T) getParser().parseDelimitedFrom(inputStream);
//            if (current == null) {
//                LOG.debug("SOURCE parseDelimited returned null");
//                if (!openNextFile()) {
//                    return false;
//                }
//                current = (T) getParser().parseDelimitedFrom(inputStream);
//            }
//            return (current != null);
//        }
//
//        @Override
//        public T getCurrent() throws NoSuchElementException {
//            return current;
//        }
//
//        @Override
//        public void close() throws IOException {
//            inputStream.close();
//        }
//
//        @Override
//        public BoundedSource<T> getCurrentSource() {
//            return source;
//        }
//
//        private boolean openNextFile() throws IOException {
//            if (inputStream != null) {
//                inputStream.close();
//            }
//            ++fileIdx;
//            if (fileIdx >= source.files.size()) {
//                LOG.debug("SOURCE: no more files to open");
//                return false;
//            }
//            fileDesc = (FileDesc) source.files.get(fileIdx);
//            fileChannel = FileChannel.open(Paths.get(fileDesc.name));
//            if (fileDesc.len > 0) {
//                untilByte = fileDesc.offset + fileDesc.len;
//            } else {
//                untilByte = fileChannel.size();
////                System.out.println("file size: " + untilByte);
//            }
//            fileChannel.position(fileDesc.offset);
//            inputStream = Channels.newInputStream(fileChannel);
//            if (fileDesc.name.endsWith(".gzip") || fileDesc.name.endsWith(".gz")) {
//                inputStream = new GZIPInputStream(inputStream);
//            }
//            LOG.info("SOURCE opened " + fileDesc.name);
//            return true;
//        }
//    }

    public static class Sink<T extends Message> extends FileBasedSink<T> implements Serializable {
        public Sink(ValueProvider<ResourceId> baseOutputDirectoryProvider, FilenamePolicy filenamePolicy) {
            super(baseOutputDirectoryProvider, filenamePolicy);
        }

        public Sink(ValueProvider<ResourceId> baseOutputDirectoryProvider, FilenamePolicy filenamePolicy,
                             FileBasedSink.WritableByteChannelFactory writableByteChannelFactory) {
            super(baseOutputDirectoryProvider, filenamePolicy, writableByteChannelFactory);
        }

        @Override
        public WriteOperation<T> createWriteOperation() {
            return new ProtoIOWriteOperation(this, getBaseOutputDirectoryProvider());
        }
    }

    public static class ProtoIOWriteOperation<T extends Message> extends FileBasedSink.WriteOperation {
        public ProtoIOWriteOperation(FileBasedSink<T> sink) {
            super(sink);
        }

        public ProtoIOWriteOperation(FileBasedSink<T> sink, ResourceId tempDirectory) {
            super(sink, tempDirectory);
        }

        public ProtoIOWriteOperation(FileBasedSink<T> sink, ValueProvider<ResourceId> tempDirectory) {
            // TODO make the superclass constructor public!
            super(sink, tempDirectory.get());
        }

        @Override
        public FileBasedSink.Writer createWriter() throws Exception {
            return new ProtoIOWriter(this);
        }
    }

    public static class ProtoIOWriter<T extends Message> extends FileBasedSink.Writer<T> {
        OutputStream outputStream;

        public ProtoIOWriter(FileBasedSink.WriteOperation<T> writeOperation) {
            super(writeOperation, MimeTypes.BINARY);
        }

        @Override
        protected void prepareWrite(WritableByteChannel writableByteChannel) throws Exception {
            this.outputStream = Channels.newOutputStream(writableByteChannel);
        }

        @Override
        public void write(T t) throws Exception {
            t.writeDelimitedTo(outputStream);
        }
    }
}