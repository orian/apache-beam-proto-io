package eu.pawelsz.apache.beam.io.protoio;

import com.google.protobuf.Message;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.util.MimeTypes;

import java.io.OutputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

public class ProtoIOSink<DestinationT, T extends Message>
        extends FileBasedSink
        implements Serializable {

    public ProtoIOSink(ValueProvider<ResourceId> tempDirectoryProvider,
                       DynamicDestinations<T, DestinationT, T> dynamicDestinations) {
        super(tempDirectoryProvider, dynamicDestinations);
    }

    public ProtoIOSink(ValueProvider<ResourceId> tempDirectoryProvider,
                       DynamicDestinations<T, DestinationT, T> dynamicDestinations,
                       WritableByteChannelFactory writableByteChannelFactory) {
        super(tempDirectoryProvider, dynamicDestinations, writableByteChannelFactory);
    }

    public ProtoIOSink(ValueProvider<ResourceId> tempDirectoryProvider,
                       DynamicDestinations<T, DestinationT, T> dynamicDestinations,
                       Compression compression) {
        super(tempDirectoryProvider, dynamicDestinations, compression);
    }

    @Override
    public WriteOperation createWriteOperation() {
        return new ProtoIOWriteOperation(this);
    }

    public static class ProtoIOWriteOperation<DestinationT, T extends Message>
            extends WriteOperation<DestinationT, T> {
        public ProtoIOWriteOperation(ProtoIOSink<DestinationT, T> sink) {
            super(sink);
        }

        @Override
        public Writer createWriter() throws Exception {
            return new ProtoIOWriter(this);
        }
    }

    public static class ProtoIOWriter<DestinationT, T extends Message> extends Writer<DestinationT, T> {
        OutputStream outputStream;

        public ProtoIOWriter(WriteOperation<DestinationT, T> writeOperation) {
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

        @Override
        protected void finishWrite() throws Exception {
            outputStream.flush();
        }
    }
}
