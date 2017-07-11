package eu.pawelsz.apache.beam.io.protoio;

import com.google.protobuf.Message;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtoIO {
    private static final Logger LOG = LoggerFactory.getLogger(ProtoIO.class);

    public static <T extends Message> ProtoIOSource<T> source (Class<T> recordClass, String fileOrPatternSpec) {
        return ProtoIOSource.from(recordClass, fileOrPatternSpec);
    }

    public static <T extends Message> ProtoIOSource<T> source (Class<T> recordClass, ValueProvider<String> fileOrPatternSpec) {
        return ProtoIOSource.from(recordClass, fileOrPatternSpec);
    }

    public static <T extends Message> ProtoIOSink<T> sink(String baseOutputFilename) {
        ResourceId resourceId = FileBasedSink.convertToFileResourceIfPossible(baseOutputFilename);
        ValueProvider<ResourceId> valueProvider = ValueProvider.StaticValueProvider.of(resourceId);

        return new ProtoIOSink<T>(valueProvider,
                DefaultFilenamePolicy.constructUsingStandardParameters(
                        valueProvider, null, null));
    }

    public static <T extends Message> ProtoIOSink<T> sink(String baseOutputFilename, String extension,
                                                          FileBasedSink.WritableByteChannelFactory factory) {

        ResourceId resourceId = FileBasedSink.convertToFileResourceIfPossible(baseOutputFilename);
        ValueProvider<ResourceId> valueProvider = ValueProvider.StaticValueProvider.of(resourceId);

        return new ProtoIOSink<T>(valueProvider,
                DefaultFilenamePolicy.constructUsingStandardParameters(
                        valueProvider, null, extension),
                factory);
    }

    public static <T extends Message> ProtoIOSink<T> sink(ValueProvider<String> baseOutputFilename,
                                                          String extension,
                                                          FileBasedSink.WritableByteChannelFactory factory) {
        ValueProvider<ResourceId> valueProvider = ValueProvider.NestedValueProvider.of(
                baseOutputFilename, FileBasedSink::convertToFileResourceIfPossible);

        return new ProtoIOSink<T>(valueProvider,
                DefaultFilenamePolicy.constructUsingStandardParameters(
                        valueProvider, null, extension),
                factory);
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
}