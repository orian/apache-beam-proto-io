package eu.pawelsz.apache.beam.io.protoio;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.protobuf.Message;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.sdk.io.AvroIO.constantDestinations;

public class ProtoIO {
    private static final Logger LOG = LoggerFactory.getLogger(ProtoIO.class);

    /**
     * A {@link PTransform} that reads from one or more text files and returns a bounded
     * {@link PCollection} containing one element for each line of the input files.
     */
    public static <T extends Message> Read<T> read(Class<T> recordClass) {
        return new AutoValue_ProtoIO_Read.Builder()
                .setCompression(Compression.AUTO)
                .setHintMatchesManyFiles(false)
                .setMatchConfiguration(FileIO.MatchConfiguration.create(EmptyMatchTreatment.DISALLOW))
                .setRecordClass(recordClass)
                .build();
    }

    /**
     * A {@link PTransform} that works like {@link #read}, but reads each file in a {@link
     * PCollection} of filepatterns.
     *
     * <p>Can be applied to both bounded and unbounded {@link PCollection PCollections}, so this is
     * suitable for reading a {@link PCollection} of filepatterns arriving as a stream. However, every
     * filepattern is expanded once at the moment it is processed, rather than watched for new files
     * matching the filepattern to appear. Likewise, every file is read once, rather than watched for
     * new entries.
     */
    public static <T extends Message> ReadAll<T> readAll(Class<T> recordClass) {
        return new AutoValue_ProtoIO_ReadAll.Builder()
                .setCompression(Compression.AUTO)
                .setMatchConfiguration(FileIO.MatchConfiguration.create(EmptyMatchTreatment.ALLOW_IF_WILDCARD))
                .setRecordClass(recordClass)
                .build();
    }


    /**
     * Writes a {@link PCollection} to an Avro file (or multiple Avro files matching a sharding
     * pattern).
     */
    public static <T extends Message> Write<T> write(Class<T> recordClass) {
        return new Write<>(
                ProtoIO.<T, T>defaultWriteBuilder()
                        .setRecordClass(recordClass)
                        .build());
    }


    public static <T extends Message> ProtoSource<T> source (Class<T> recordClass, String fileOrPatternSpec) {
        return ProtoSource.from(recordClass, fileOrPatternSpec);
    }
//
//    public static <T extends Message> ProtoSource<T> source (Class<T> recordClass, ValueProvider<String> fileOrPatternSpec) {
//        return ProtoSource.from(recordClass, fileOrPatternSpec);
//    }
//
//    public static <T extends Message> ProtoSink<Void,T> sink(String baseOutputFilename) {
//        ResourceId resourceId = FileBasedSink.convertToFileResourceIfPossible(baseOutputFilename);
//        ValueProvider<ResourceId> valueProvider = ValueProvider.StaticValueProvider.of(resourceId);
//
//        // In this case, DestinationT == Void
//        FileBasedSink.FilenamePolicy usedFilenamePolicy =
//                    DefaultFilenamePolicy.fromStandardParameters(
//                            valueProvider,
//                            null,null,
//                            false);
//
//        usedFilenamePolicy = DefaultFilenamePolicy.fromParams(new DefaultFilenamePolicy.Params().withBaseFilename(resourceId));
//        FileBasedSink.DynamicDestinations dynamicDestinations = DynamicFileDestinations.constant(usedFilenamePolicy);
////        dynamicDestinations =
////                (FileBasedSink.DynamicDestinations)
////                        DynamicFileDestinations.constant(usedFilenamePolicy, getFormatFunction());
//
//        return new ProtoSink<Void, T>(valueProvider, dynamicDestinations);
//    }

//    public static <T extends Message> ProtoSink<Void, T> sink(String baseOutputFilename, String extension,
//                                                              FileBasedSink.WritableByteChannelFactory factory) {
//
//        ResourceId resourceId = FileBasedSink.convertToFileResourceIfPossible(baseOutputFilename);
//        ValueProvider<ResourceId> valueProvider = ValueProvider.StaticValueProvider.of(resourceId);
//
//        DefaultFilenamePolicy usedFilenamePolicy = DefaultFilenamePolicy.fromParams(
//                new DefaultFilenamePolicy.Params().withBaseFilename(resourceId).withSuffix(extension));
//
//        FileBasedSink.DynamicDestinations dynamicDestinations = DynamicFileDestinations.constant(usedFilenamePolicy);
//
//        return new ProtoSink<Void, T>(valueProvider, dynamicDestinations, factory);
//    }
//
//    public static <T extends Message> ProtoSink<Void, T> sink(ValueProvider<String> baseOutputFilename,
//                                                              String extension,
//                                                              FileBasedSink.WritableByteChannelFactory factory) {
//        ValueProvider<ResourceId> valueProvider = ValueProvider.NestedValueProvider.of(
//                baseOutputFilename, FileBasedSink::convertToFileResourceIfPossible);
//
//        DefaultFilenamePolicy usedFilenamePolicy = DefaultFilenamePolicy.fromParams(
//                new DefaultFilenamePolicy.Params().withBaseFilename(valueProvider).withSuffix(extension));
//
//        FileBasedSink.DynamicDestinations dynamicDestinations = DynamicFileDestinations.constant(usedFilenamePolicy);
//
//        return new ProtoSink<Void, T>(valueProvider, dynamicDestinations, factory);
//    }

//    public static <T extends Message> PTransform<PCollection<T>, PDone> write(
//            String baseOutputFilename) {
//        return WriteFiles.<T,Void,T>to(sink(baseOutputFilename));
//    }
//
//    public static <T extends Message> PTransform<PCollection<T>, PDone> write(
//            String baseOutputFilename, String extension,
//            FileBasedSink.WritableByteChannelFactory factory) {
//        return WriteFiles.<T,Void,T>to(sink(baseOutputFilename, extension, factory));
//    }
//
//    public static <T extends Message> PTransform<PCollection<T>, PDone> write(
//            ValueProvider<String> baseOutputFilename, String extension,
//            FileBasedSink.WritableByteChannelFactory factory) {
//        return WriteFiles.<T,Void,T>to(sink(baseOutputFilename, extension, factory));
//    }

    private static <UserT, OutputT extends Message> TypedWrite.Builder<UserT, Void, OutputT> defaultWriteBuilder() {
        return new AutoValue_ProtoIO_TypedWrite.Builder<UserT, Void, OutputT>()
                .setFilenameSuffix(null)
                .setCompression(Compression.UNCOMPRESSED)
                .setShardTemplate(null)
                .setNumShards(0)
                .setMetadata(ImmutableMap.of())
                .setWindowedWrites(false);
    }

    @AutoValue
    public abstract static class Read<T extends Message> extends PTransform<PBegin, PCollection<T>> {
        @Nullable abstract ValueProvider<String> getFilepattern();
        abstract FileIO.MatchConfiguration getMatchConfiguration();
        @Nullable abstract Class<T> getRecordClass();
        abstract Compression getCompression();
        abstract boolean getHintMatchesManyFiles();

        abstract Builder<T> toBuilder();

        @AutoValue.Builder
        abstract static class Builder<T extends Message> {
            abstract Builder<T> setFilepattern(ValueProvider<String> filepattern);
            abstract Builder<T> setMatchConfiguration(FileIO.MatchConfiguration matchConfiguration);
            abstract Builder<T> setRecordClass(Class<T> recordClass);
            abstract Builder<T> setCompression(Compression compression);
            abstract Builder<T> setHintMatchesManyFiles(boolean hintManyFiles);

            abstract Read<T> build();
        }


        /**
         * Reads from the given filename or filepattern.
         *
         * <p>If it is known that the filepattern will match a very large number of files (at least tens
         * of thousands), use {@link #withHintMatchesManyFiles} for better performance and scalability.
         */
        public Read<T> from(ValueProvider<String> filepattern) {
            return toBuilder().setFilepattern(filepattern).build();
        }

        /** Like {@link #from(ValueProvider)}. */
        public Read<T> from(String filepattern) {
            return from(ValueProvider.StaticValueProvider.of(filepattern));
        }

//
//
//        /**
//         * Reads text files that reads from the file(s) with the given filename or filename pattern.
//         *
//         * <p>This can be a local path (if running locally), or a Google Cloud Storage filename or
//         * filename pattern of the form {@code "gs://<bucket>/<filepath>"} (if running locally or using
//         * remote execution service).
//         *
//         * <p>Standard <a href="http://docs.oracle.com/javase/tutorial/essential/io/find.html" >Java
//         * Filesystem glob patterns</a> ("*", "?", "[..]") are supported.
//         *
//         * <p>If it is known that the filepattern will match a very large number of files (at least tens
//         * of thousands), use {@link #withHintMatchesManyFiles} for better performance and scalability.
//         */
//        public ProtoIO.Read from(String filepattern) {
//            checkArgument(filepattern != null, "filepattern can not be null");
//            return from(ValueProvider.StaticValueProvider.of(filepattern));
//        }
//
//        /** Same as {@code from(filepattern)}, but accepting a {@link ValueProvider}. */
//        public ProtoIO.Read from(ValueProvider<String> filepattern) {
//            checkArgument(filepattern != null, "filepattern can not be null");
//            return toBuilder().setFilepattern(filepattern).build();
//        }

        /** Sets the {@link FileIO.MatchConfiguration}. */
        public ProtoIO.Read withMatchConfiguration(FileIO.MatchConfiguration matchConfiguration) {
            return toBuilder().setMatchConfiguration(matchConfiguration).build();
        }

        /** Configures whether or not a filepattern matching no files is allowed. */
        public Read<T> withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
            return withMatchConfiguration(getMatchConfiguration().withEmptyMatchTreatment(treatment));
        }


        /**
         * Reads from input sources using the specified compression type.
         *
         * <p>If no compression type is specified, the default is {@link Compression#AUTO}.
         */
        public ProtoIO.Read withCompression(Compression compression) {
            return toBuilder().setCompression(compression).build();
        }

        /**
         * See {@link FileIO.MatchConfiguration#continuously}.
         *
         * <p>This works only in runners supporting {@link Experimental.Kind#SPLITTABLE_DO_FN}.
         */
        @Experimental(Experimental.Kind.SPLITTABLE_DO_FN)
        public ProtoIO.Read watchForNewFiles(
                Duration pollInterval, Watch.Growth.TerminationCondition<String, ?> terminationCondition) {
            return withMatchConfiguration(
                    getMatchConfiguration().continuously(pollInterval, terminationCondition));
        }

        /**
         * Hints that the filepattern specified in {@link #from(String)} matches a very large number of
         * files.
         *
         * <p>This hint may cause a runner to execute the transform differently, in a way that improves
         * performance for this case, but it may worsen performance if the filepattern matches only
         * a small number of files (e.g., in a runner that supports dynamic work rebalancing, it will
         * happen less efficiently within individual files).
         */
        public ProtoIO.Read withHintMatchesManyFiles() {
            return toBuilder().setHintMatchesManyFiles(true).build();
        }

        @Override
        public PCollection<T> expand(PBegin input) {
            checkNotNull(getFilepattern(), "filepattern");
            checkNotNull(getRecordClass(), "recordClass");

//            if (getMatchConfiguration().getWatchInterval() == null && !getHintMatchesManyFiles()) {
                return input.apply(
                        "Read",
                        org.apache.beam.sdk.io.Read.from(
                                createSource(
                                        getFilepattern(),
                                        /*getMatchConfiguration().getEmptyMatchTreatment()*/
                                        EmptyMatchTreatment.DISALLOW,
                                        getRecordClass())));
//            }
//            ReadAll<T> ra = readAll(getRecordClass())
//                    .withCompression(getCompression())
//                    .withMatchConfiguration(getMatchConfiguration());
//            return input
//                    .apply("Create filepattern", Create.ofProvider(getFilepattern(), StringUtf8Coder.of()))
//                    .apply("Via ReadAll", ra);
        }

//        // Helper to create a source specific to the requested compression type.
//        protected FileBasedSource<T> getSource() {
//            return CompressedSource.from(
//                    new ProtoSource(
//                            getFilepattern(),
//                            getMatchConfiguration().getEmptyMatchTreatment(),
//                            getDelimiter()))
//                    .withCompression(getCompression());
//        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            builder
                    .add(
                            DisplayData.item("compressionType", getCompression().toString())
                                    .withLabel("Compression Type"))
                    .addIfNotNull(
                            DisplayData.item("filePattern", getFilepattern()).withLabel("Input File Pattern"))
                    .include("matchConfiguration", getMatchConfiguration());
        }

        @SuppressWarnings("unchecked")
        private static <T extends Message> ProtoSource<T> createSource(
                ValueProvider<String> filepattern,
                EmptyMatchTreatment emptyMatchTreatment,
                Class<T> recordClass) {
            return ProtoSource.from(recordClass, filepattern);
        }
    }

    /** Implementation of {@link #readAll}. */
    @AutoValue
    public abstract static class ReadAll<T extends Message>
            extends PTransform<PCollection<String>, PCollection<T>> {
        abstract FileIO.MatchConfiguration getMatchConfiguration();
        abstract Compression getCompression();
        @Nullable abstract Class<T> getRecordClass();
        abstract long getDesiredBundleSizeBytes();

        abstract Builder<T> toBuilder();

        @AutoValue.Builder
        abstract static class Builder<T extends Message> {
            abstract Builder<T> setMatchConfiguration(FileIO.MatchConfiguration matchConfiguration);
            abstract Builder<T> setCompression(Compression compression);
            abstract Builder<T> setRecordClass(Class<T> recordClass);
            abstract Builder<T> setDesiredBundleSizeBytes(long desiredBundleSizeBytes);

            abstract ReadAll<T> build();
        }

        /** Sets the {@link FileIO.MatchConfiguration}. */
        public ReadAll<T> withMatchConfiguration(FileIO.MatchConfiguration configuration) {
            return toBuilder().setMatchConfiguration(configuration).build();
        }

        @VisibleForTesting
        ReadAll<T> withDesiredBundleSizeBytes(long desiredBundleSizeBytes) {
            return toBuilder().setDesiredBundleSizeBytes(desiredBundleSizeBytes).build();
        }

        /**
         * Reads from input sources using the specified compression type.
         *
         * <p>If no compression type is specified, the default is {@link Compression#AUTO}.
         */
        public ReadAll<T> withCompression(Compression compression) {
            return toBuilder().setCompression(compression).build();
        }

        /** Same as {@link Read#withEmptyMatchTreatment}. */
        public ReadAll<T> withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
            return withMatchConfiguration(getMatchConfiguration().withEmptyMatchTreatment(treatment));
        }

        /** Same as {@link Read#watchForNewFiles(Duration, Watch.Growth.TerminationCondition)}. */
        @Experimental(Experimental.Kind.SPLITTABLE_DO_FN)
        public ReadAll<T> watchForNewFiles(
                Duration pollInterval, Watch.Growth.TerminationCondition<String, ?> terminationCondition) {
            return withMatchConfiguration(
                    getMatchConfiguration().continuously(pollInterval, terminationCondition));
        }

        @Override
        public PCollection<T> expand(PCollection<String> input) {
            return input
                    .apply(FileIO.matchAll().withConfiguration(getMatchConfiguration()))
                    .apply(FileIO.readMatches()/*.withDirectoryTreatment(DirectoryTreatment.PROHIBIT)*/)
                    .apply("Read all via FileBasedSource",
                            new ReadAllViaFileBasedSource<>(
                                    getDesiredBundleSizeBytes(), new CreateSourceFn<>(getRecordClass()),
                                    ProtoCoder.of(getRecordClass())));
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            builder.add(
                    DisplayData.item("compressionType", getCompression().toString())
                            .withLabel("Compression Type"))
                    .include("matchConfiguration", getMatchConfiguration());
        }
    }

    private static class CreateSourceFn<T extends Message>
            implements SerializableFunction<String, FileBasedSource<T>> {
        private final Class<T> recordClass;

        public CreateSourceFn(Class<T> recordClass) {
            this.recordClass = recordClass;
        }

        @Override
        public FileBasedSource<T> apply(String input) {
            return Read.createSource(
                    ValueProvider.StaticValueProvider.of(input),
                    EmptyMatchTreatment.DISALLOW,
                    recordClass);
        }
    }

    ///////////////////////////////////////////////////////////////////////////

    /** Implementation of {@link #write}. */
    @AutoValue
    public abstract static class TypedWrite<UserT, DestinationT, OutputT extends Message>
            extends PTransform<PCollection<UserT>, WriteFilesResult<DestinationT>> {

        @Nullable
        abstract SerializableFunction<UserT, OutputT> getFormatFunction();

        @Nullable abstract ValueProvider<ResourceId> getFilenamePrefix();
        @Nullable abstract String getShardTemplate();
        @Nullable abstract String getFilenameSuffix();
        abstract Class<OutputT> getRecordClass();

        @Nullable
        abstract ValueProvider<ResourceId> getTempDirectory();

        abstract int getNumShards();
        abstract Compression getCompression();

        abstract boolean getWindowedWrites();
        @Nullable abstract FileBasedSink.FilenamePolicy getFilenamePolicy();

        @Nullable
        abstract DynamicProtoDestinations<UserT, DestinationT, OutputT> getDynamicDestinations();

        /** Avro file metadata. */
        abstract ImmutableMap<String, Object> getMetadata();

        abstract Builder<UserT, DestinationT, OutputT> toBuilder();

        @AutoValue.Builder
        abstract static class Builder<UserT, DestinationT, OutputT extends Message> {
            abstract Builder<UserT, DestinationT, OutputT> setFormatFunction(
                    @Nullable SerializableFunction<UserT, OutputT> formatFunction);

            abstract Builder<UserT, DestinationT, OutputT> setFilenamePrefix(
                    ValueProvider<ResourceId> filenamePrefix);

            abstract Builder<UserT, DestinationT, OutputT> setFilenameSuffix(String filenameSuffix);

            abstract Builder<UserT, DestinationT, OutputT> setTempDirectory(
                    ValueProvider<ResourceId> tempDirectory);

            abstract Builder<UserT, DestinationT, OutputT> setNumShards(int numShards);
            abstract Builder<UserT, DestinationT, OutputT> setCompression(Compression compression);

            abstract Builder<UserT, DestinationT, OutputT> setShardTemplate(
                    @Nullable String shardTemplate);

            abstract Builder<UserT, DestinationT, OutputT> setRecordClass(Class<OutputT> schema);

            abstract Builder<UserT, DestinationT, OutputT> setWindowedWrites(boolean windowedWrites);

            abstract Builder<UserT, DestinationT, OutputT> setFilenamePolicy(
                    FileBasedSink.FilenamePolicy filenamePolicy);

            abstract Builder<UserT, DestinationT, OutputT> setMetadata(
                    ImmutableMap<String, Object> metadata);

            abstract Builder<UserT, DestinationT, OutputT> setDynamicDestinations(
                    DynamicProtoDestinations<UserT, DestinationT, OutputT> dynamicDestinations);

            abstract TypedWrite<UserT, DestinationT, OutputT> build();
        }

        /**
         * Writes to file(s) with the given output prefix. See {@link FileSystems} for information on
         * supported file systems.
         *
         * <p>The name of the output files will be determined by the {@link FileBasedSink.FilenamePolicy} used.s
         *
         * <p>By default, a {@link DefaultFilenamePolicy} will build output filenames using the
         * specified prefix, a shard name template (see {@link #withShardNameTemplate(String)}, and a
         * common suffix (if supplied using {@link #withSuffix(String)}). This default can be overridden
         * using {@link #to(FileBasedSink.FilenamePolicy)}.
         */
        public TypedWrite<UserT, DestinationT, OutputT> to(String outputPrefix) {
            return to(FileBasedSink.convertToFileResourceIfPossible(outputPrefix));
        }

        /**
         * Writes to file(s) with the given output prefix. See {@link FileSystems} for information on
         * supported file systems. This prefix is used by the {@link DefaultFilenamePolicy} to generate
         * filenames.
         *
         * <p>By default, a {@link DefaultFilenamePolicy} will build output filenames using the
         * specified prefix, a shard name template (see {@link #withShardNameTemplate(String)}, and a
         * common suffix (if supplied using {@link #withSuffix(String)}). This default can be overridden
         * using {@link #to(FileBasedSink.FilenamePolicy)}.
         *
         * <p>This default policy can be overridden using {@link #to(FileBasedSink.FilenamePolicy)}, in which case
         * {@link #withShardNameTemplate(String)} and {@link #withSuffix(String)} should not be set.
         * Custom filename policies do not automatically see this prefix - you should explicitly pass
         * the prefix into your {@link FileBasedSink.FilenamePolicy} object if you need this.
         *
         * <p>If {@link #withTempDirectory} has not been called, this filename prefix will be used to
         * infer a directory for temporary files.
         */
        @Experimental(Experimental.Kind.FILESYSTEM)
        public TypedWrite<UserT, DestinationT, OutputT> to(ResourceId outputPrefix) {
            return toResource(ValueProvider.StaticValueProvider.of(outputPrefix));
        }

        private static class OutputPrefixToResourceId
                implements SerializableFunction<String, ResourceId> {
            @Override
            public ResourceId apply(String input) {
                return FileBasedSink.convertToFileResourceIfPossible(input);
            }
        }

        /** Like {@link #to(String)}. */
        public TypedWrite<UserT, DestinationT, OutputT> to(ValueProvider<String> outputPrefix) {
            return toResource(
                    ValueProvider.NestedValueProvider.of(
                            outputPrefix,
                            // The function cannot be created as an anonymous class here since the enclosed class
                            // may contain unserializable members.
                            new OutputPrefixToResourceId()));
        }

        /** Like {@link #to(ResourceId)}. */
        @Experimental(Experimental.Kind.FILESYSTEM)
        public TypedWrite<UserT, DestinationT, OutputT> toResource(
                ValueProvider<ResourceId> outputPrefix) {
            return toBuilder().setFilenamePrefix(outputPrefix).build();
        }

        /**
         * Writes to files named according to the given {@link FileBasedSink.FilenamePolicy}. A
         * directory for temporary files must be specified using {@link #withTempDirectory}.
         */
        @Experimental(Experimental.Kind.FILESYSTEM)
        public TypedWrite<UserT, DestinationT, OutputT> to(FileBasedSink.FilenamePolicy filenamePolicy) {
            return toBuilder().setFilenamePolicy(filenamePolicy).build();
        }

        /**
         * Use a {@link DynamicProtoDestinations} object to vend {@link FileBasedSink.FilenamePolicy} objects. These
         * objects can examine the input record when creating a {@link FileBasedSink.FilenamePolicy}. A directory for
         * temporary files must be specified using {@link #withTempDirectory}.
         *
         * @deprecated Use {@link FileIO#write()} or {@link FileIO#writeDynamic()} instead.
         */
        @Experimental(Experimental.Kind.FILESYSTEM)
        @Deprecated
        public <NewDestinationT> TypedWrite<UserT, NewDestinationT, OutputT> to(
                DynamicProtoDestinations<UserT, NewDestinationT, OutputT> dynamicDestinations) {
            return toBuilder()
                    .setDynamicDestinations((DynamicProtoDestinations) dynamicDestinations)
                    .build();
        }

        /**
         * Specifies a format function to convert {@link UserT} to the output type. If {@link
         * #to(DynamicProtoDestinations)} is used, {@link DynamicProtoDestinations#formatRecord} must be
         * used instead.
         */
        public TypedWrite<UserT, DestinationT, OutputT> withFormatFunction(
                @Nullable SerializableFunction<UserT, OutputT> formatFunction) {
            return toBuilder().setFormatFunction(formatFunction).build();
        }

        /** Set the base directory used to generate temporary files. */
        @Experimental(Experimental.Kind.FILESYSTEM)
        public TypedWrite<UserT, DestinationT, OutputT> withTempDirectory(
                ValueProvider<ResourceId> tempDirectory) {
            return toBuilder().setTempDirectory(tempDirectory).build();
        }

        /** Set the base directory used to generate temporary files. */
        @Experimental(Experimental.Kind.FILESYSTEM)
        public TypedWrite<UserT, DestinationT, OutputT> withTempDirectory(ResourceId tempDirectory) {
            return withTempDirectory(ValueProvider.StaticValueProvider.of(tempDirectory));
        }

        /**
         * Uses the given {@link ShardNameTemplate} for naming output files. This option may only be
         * used when using one of the default filename-prefix to() overrides.
         *
         * <p>See {@link DefaultFilenamePolicy} for how the prefix, shard name template, and suffix are
         * used.
         */
        public TypedWrite<UserT, DestinationT, OutputT> withShardNameTemplate(String shardTemplate) {
            return toBuilder().setShardTemplate(shardTemplate).build();
        }

        /**
         * Configures the filename suffix for written files. This option may only be used when using one
         * of the default filename-prefix to() overrides.
         *
         * <p>See {@link DefaultFilenamePolicy} for how the prefix, shard name template, and suffix are
         * used.
         */
        public TypedWrite<UserT, DestinationT, OutputT> withSuffix(String filenameSuffix) {
            return toBuilder().setFilenameSuffix(filenameSuffix).build();
        }

        /**
         * Configures the number of output shards produced overall (when using unwindowed writes) or
         * per-window (when using windowed writes).
         *
         * <p>For unwindowed writes, constraining the number of shards is likely to reduce the
         * performance of a pipeline. Setting this value is not recommended unless you require a
         * specific number of output files.
         *
         * @param numShards the number of shards to use, or 0 to let the system decide.
         */
        public TypedWrite<UserT, DestinationT, OutputT> withNumShards(int numShards) {
            checkArgument(numShards >= 0);
            return toBuilder().setNumShards(numShards).build();
        }

        /**
         * Forces a single file as output and empty shard name template. This option is only compatible
         * with unwindowed writes.
         *
         * <p>For unwindowed writes, constraining the number of shards is likely to reduce the
         * performance of a pipeline. Setting this value is not recommended unless you require a
         * specific number of output files.
         *
         * <p>This is equivalent to {@code .withNumShards(1).withShardNameTemplate("")}
         */
        public TypedWrite<UserT, DestinationT, OutputT> withoutSharding() {
            return withNumShards(1).withShardNameTemplate("");
        }

        /**
         * Preserves windowing of input elements and writes them to files based on the element's window.
         *
         * <p>If using {@link #to(FileBasedSink.FilenamePolicy)}. Filenames will be generated using
         * {@link FileBasedSink.FilenamePolicy#windowedFilename}. See also {@link WriteFiles#withWindowedWrites()}.
         */
        public TypedWrite<UserT, DestinationT, OutputT> withWindowedWrites() {
            return toBuilder().setWindowedWrites(true).build();
        }

        public TypedWrite<UserT, DestinationT, OutputT> withCompression(Compression compression) {
            return toBuilder().setCompression(compression).build();
        }

        public TypedWrite<UserT, DestinationT, OutputT> withRecordClass(Class<OutputT> recordClass) {
            return toBuilder().setRecordClass(recordClass).build();
        }

        /**
         * Writes to Avro file(s) with the specified metadata.
         *
         * <p>Supported value types are String, Long, and byte[].
         */
        public TypedWrite<UserT, DestinationT, OutputT> withMetadata(Map<String, Object> metadata) {
            Map<String, String> badKeys = Maps.newLinkedHashMap();
            for (Map.Entry<String, Object> entry : metadata.entrySet()) {
                Object v = entry.getValue();
                if (!(v instanceof String || v instanceof Long || v instanceof byte[])) {
                    badKeys.put(entry.getKey(), v.getClass().getSimpleName());
                }
            }
            checkArgument(
                    badKeys.isEmpty(),
                    "Metadata value type must be one of String, Long, or byte[]. Found {}",
                    badKeys);
            return toBuilder().setMetadata(ImmutableMap.copyOf(metadata)).build();
        }

        DynamicProtoDestinations<UserT, DestinationT, OutputT> resolveDynamicDestinations() {
            DynamicProtoDestinations<UserT, DestinationT, OutputT> dynamicDestinations =
                    getDynamicDestinations();
            if (dynamicDestinations == null) {
                // In this case DestinationT is Void.
                FileBasedSink.FilenamePolicy usedFilenamePolicy = getFilenamePolicy();
                if (usedFilenamePolicy == null) {
                    usedFilenamePolicy =
                            DefaultFilenamePolicy.fromStandardParameters(
                                    getFilenamePrefix(),
                                    getShardTemplate(),
                                    getFilenameSuffix(),
                                    getWindowedWrites());
                }
                dynamicDestinations =
                        (DynamicProtoDestinations<UserT, DestinationT, OutputT>)
                                ProtoIO.<UserT, OutputT>constantDestinations(
                                        usedFilenamePolicy,
                                        getMetadata(),
                                        getFormatFunction());
            }

            return dynamicDestinations;
        }

        @Override
        public WriteFilesResult<DestinationT> expand(PCollection<UserT> input) {
            checkArgument(
                    getFilenamePrefix() != null || getTempDirectory() != null,
                    "Need to set either the filename prefix or the tempDirectory of a ProtoIO.Write "
                            + "transform.");
            if (getFilenamePolicy() != null) {
                checkArgument(
                        getShardTemplate() == null && getFilenameSuffix() == null,
                        "shardTemplate and filenameSuffix should only be used with the default "
                                + "filename policy");
            }
            if (getDynamicDestinations() != null) {
                checkArgument(
                        getFormatFunction() == null,
                        "A format function should not be specified "
                                + "with DynamicDestinations. Use DynamicDestinations.formatRecord instead");
            } else {
                checkArgument(
                        getRecordClass() != null, "Unless using DynamicDestinations, .withSchema() is required.");
            }

            ValueProvider<ResourceId> tempDirectory = getTempDirectory();
            if (tempDirectory == null) {
                tempDirectory = getFilenamePrefix();
            }
            WriteFiles<UserT, DestinationT, OutputT> write =
                    WriteFiles.to(
                            new ProtoSink<>(tempDirectory, resolveDynamicDestinations(), getCompression()));
            if (getNumShards() > 0) {
                write = write.withNumShards(getNumShards());
            }
            if (getWindowedWrites()) {
                write = write.withWindowedWrites();
            }
            return input.apply("Write", write);
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            resolveDynamicDestinations().populateDisplayData(builder);
            builder
                    .addIfNotDefault(
                            DisplayData.item("numShards", getNumShards()).withLabel("Maximum Output Shards"), 0)
                    .addIfNotNull(
                            DisplayData.item("tempDirectory", getTempDirectory())
                                    .withLabel("Directory for temporary files"));
        }
    }

    /**
     * This class is used as the default return value of {@link AvroIO#write}
     *
     * <p>All methods in this class delegate to the appropriate method of {@link AvroIO.TypedWrite}.
     * This class exists for backwards compatibility, and will be removed in Beam 3.0.
     */
    public static class Write<T extends Message> extends PTransform<PCollection<T>, PDone> {
        @VisibleForTesting TypedWrite<T, ?, T> inner;

        Write(TypedWrite<T, ?, T> inner) {
            this.inner = inner;
        }

        /** See {@link TypedWrite#to(String)}. */
        public Write<T> to(String outputPrefix) {
            return new Write<>(
                    inner
                            .to(FileBasedSink.convertToFileResourceIfPossible(outputPrefix))
                            .withFormatFunction(SerializableFunctions.identity()));
        }

        /** See {@link TypedWrite#to(ResourceId)} . */
        @Experimental(Experimental.Kind.FILESYSTEM)
        public Write<T> to(ResourceId outputPrefix) {
            return new Write<>(
                    inner.to(outputPrefix).withFormatFunction(SerializableFunctions.identity()));
        }

        /** See {@link TypedWrite#to(ValueProvider)}. */
        public Write<T> to(ValueProvider<String> outputPrefix) {
            return new Write<>(
                    inner.to(outputPrefix).withFormatFunction(SerializableFunctions.identity()));
        }

        /** See {@link TypedWrite#to(ResourceId)}. */
        @Experimental(Experimental.Kind.FILESYSTEM)
        public Write<T> toResource(ValueProvider<ResourceId> outputPrefix) {
            return new Write<>(
                    inner.toResource(outputPrefix).withFormatFunction(SerializableFunctions.identity()));
        }

        /** See {@link TypedWrite#to(FileBasedSink.FilenamePolicy)}. */
        public Write<T> to(FileBasedSink.FilenamePolicy filenamePolicy) {
            return new Write<>(
                    inner.to(filenamePolicy).withFormatFunction(SerializableFunctions.identity()));
        }

        /**
         * See {@link TypedWrite#to(DynamicProtoDestinations)}.
         *
         * @deprecated Use {@link FileIO#write()} or {@link FileIO#writeDynamic()} instead.
         */
        @Deprecated
        public Write<T> to(DynamicProtoDestinations<T, ?, T> dynamicDestinations) {
            return new Write<>(inner.to(dynamicDestinations).withFormatFunction(null));
        }

        /** See {@link TypedWrite#withTempDirectory(ValueProvider)}. */
        @Experimental(Experimental.Kind.FILESYSTEM)
        public Write<T> withTempDirectory(ValueProvider<ResourceId> tempDirectory) {
            return new Write<>(inner.withTempDirectory(tempDirectory));
        }

        /** See {@link TypedWrite#withTempDirectory(ResourceId)}. */
        public Write<T> withTempDirectory(ResourceId tempDirectory) {
            return new Write<>(inner.withTempDirectory(tempDirectory));
        }

        /** See {@link TypedWrite#withShardNameTemplate}. */
        public Write<T> withShardNameTemplate(String shardTemplate) {
            return new Write<>(inner.withShardNameTemplate(shardTemplate));
        }

        /** See {@link TypedWrite#withSuffix}. */
        public Write<T> withSuffix(String filenameSuffix) {
            return new Write<>(inner.withSuffix(filenameSuffix));
        }

        /** See {@link TypedWrite#withNumShards}. */
        public Write<T> withNumShards(int numShards) {
            return new Write<>(inner.withNumShards(numShards));
        }

        /** See {@link TypedWrite#withoutSharding}. */
        public Write<T> withoutSharding() {
            return new Write<>(inner.withoutSharding());
        }

        /** See {@link TypedWrite#withWindowedWrites}. */
        public Write<T> withWindowedWrites() {
            return new Write<>(inner.withWindowedWrites());
        }

        /** See {@link TypedWrite#withRecordClass}. */
        public Write<T> withRecordClass(Class<T> cls) {
            return new Write<T>(inner.withRecordClass(cls));
        }

        /** See {@link TypedWrite#withCompression}. */
        public Write<T> withCompression(Compression compression) {
            return new Write<T>(inner.withCompression(compression));
        }

        /** Specify that output filenames are wanted.
         *
         * <p>The nested {@link TypedWrite}transform always has access to output filenames, however
         * due to backwards-compatibility concerns, {@link Write} cannot return them. This method
         * simply returns the inner {@link TypedWrite} transform which has {@link WriteFilesResult} as
         * its output type, allowing access to output files.
         *
         * <p>The supplied {@code DestinationT} type must be: the same as that supplied in {@link
         * #to(DynamicProtoDestinations)} if that method was used, or {@code Void} otherwise.
         */
        public <DestinationT> TypedWrite<T, DestinationT, T> withOutputFilenames() {
            return (TypedWrite<T, DestinationT, T>) inner;
        }

        /** See {@link TypedWrite#withMetadata} . */
        public Write<T> withMetadata(Map<String, Object> metadata) {
            return new Write<>(inner.withMetadata(metadata));
        }

        @Override
        public PDone expand(PCollection<T> input) {
            input.apply(inner);
            return PDone.in(input.getPipeline());
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            inner.populateDisplayData(builder);
        }
    }

    /**
     * Returns a {@link DynamicAvroDestinations} that always returns the same {@link FileBasedSink.FilenamePolicy},
     * schema, metadata, and codec.
     */
    public static <UserT, OutputT> DynamicProtoDestinations<UserT, Void, OutputT> constantDestinations(
            FileBasedSink.FilenamePolicy filenamePolicy,
            Map<String, Object> metadata,
            SerializableFunction<UserT, OutputT> formatFunction) {
        return new ConstantProtoDestination<UserT, OutputT>(filenamePolicy, metadata, formatFunction);
    }
}