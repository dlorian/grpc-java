package de.iteconomics.grpc;

import de.iteconomics.grpc.sample.StreamRequest;
import de.iteconomics.grpc.sample.StreamResponse;
import de.iteconomics.grpc.sample.StreamServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StreamClient {

    private static final Logger logger = Logger.getLogger(StreamClient.class.getName());

    private final ManagedChannel channel;
    private final StreamServiceGrpc.StreamServiceBlockingStub blockingStub;
    private final StreamServiceGrpc.StreamServiceStub asyncStub;

    public StreamClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
    }

    public StreamClient(ManagedChannelBuilder<?> channelBuilder) {
        channel = channelBuilder.build();
        blockingStub = StreamServiceGrpc.newBlockingStub(channel);
        asyncStub = StreamServiceGrpc.newStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public CountDownLatch streamNonBlocking() {
        final CountDownLatch finishLatch = new CountDownLatch(1);

        StreamObserver<StreamResponse> responseObserver = new StreamObserver<StreamResponse>() {
            @Override
            public void onNext(StreamResponse response) {
                info("Received stream response from server {0}",response.getText());
            }

            @Override
            public void onError(Throwable t) {
                warning("Stream failed: {0}", Status.fromThrowable(t));
                finishLatch.countDown();
            }

            @Override
            public void onCompleted() {
                info("Finished stream");
                finishLatch.countDown();
            }
        };

        asyncStub.stream(StreamRequest.newBuilder().setNumber(100).build(), responseObserver);

        return finishLatch;
    }

    public static void main(String[] args) throws InterruptedException {

        StreamClient client = new StreamClient("localhost", 50051);
        try {
            logger.info("Client created and connected to localhost:50051");
            CountDownLatch finishLatch = client.streamNonBlocking();

            if (!finishLatch.await(1, TimeUnit.MINUTES)) {
                client.warning("stream can not finish within 1 minutes");
            }

            logger.info("Client request finished");
        } finally {
            client.shutdown();
        }
    }

    private void info(String msg, Object... params) {
        logger.log(Level.INFO, msg, params);
    }

    private void warning(String msg, Object... params) {
        logger.log(Level.WARNING, msg, params);
    }
}

