package de.iteconomics.grpc.service;

import de.iteconomics.grpc.StreamServer;
import de.iteconomics.grpc.sample.StreamRequest;
import de.iteconomics.grpc.sample.StreamResponse;
import de.iteconomics.grpc.sample.StreamServiceGrpc;
import io.grpc.stub.StreamObserver;

import java.util.logging.Logger;

public class StreamService extends StreamServiceGrpc.StreamServiceImplBase {

    private static final Logger logger = Logger.getLogger(StreamServer.class.getName());

    @Override
    public void stream(StreamRequest request, StreamObserver<StreamResponse> responseObserver) {
        logger.info("Received a request with number " + request.getNumber());

        int number = request.getNumber();

        try {
            for(int i = 1; i <= number; i++) {
                StreamResponse response = StreamResponse.newBuilder()
                        .setText(String.valueOf(i))
                        .build();
                responseObserver.onNext(response);

                Thread.sleep(1500L);
            }
        } catch (Exception e) {
            logger.info(e.getMessage());
        }


        responseObserver.onCompleted();
    }
}


