package com.amazonaws.kinesisvideo.parser.examples;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.kinesisvideo.parser.ebml.InputStreamParserByteSource;
import com.amazonaws.kinesisvideo.parser.mkv.MkvElementVisitException;
import com.amazonaws.kinesisvideo.parser.mkv.StreamingMkvReader;
import com.amazonaws.kinesisvideo.parser.utilities.FrameVisitor;
import com.amazonaws.kinesisvideo.parser.utilities.H264FrameReader;
import com.amazonaws.kinesisvideo.parser.utilities.SocketFrameWriter;
import com.amazonaws.kinesisvideo.parser.utilities.TestResourceUtil;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesisvideo.AmazonKinesisVideo;
import com.amazonaws.services.kinesisvideo.AmazonKinesisVideoClientBuilder;
import com.amazonaws.services.kinesisvideo.AmazonKinesisVideoMedia;
import com.amazonaws.services.kinesisvideo.AmazonKinesisVideoMediaClientBuilder;
import com.amazonaws.services.kinesisvideo.model.APIName;
import com.amazonaws.services.kinesisvideo.model.CreateStreamRequest;
import com.amazonaws.services.kinesisvideo.model.GetDataEndpointRequest;
import com.amazonaws.services.kinesisvideo.model.GetMediaRequest;
import com.amazonaws.services.kinesisvideo.model.GetMediaResult;
import com.amazonaws.services.kinesisvideo.model.StartSelector;
import com.amazonaws.services.kinesisvideo.model.StartSelectorType;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.Seconds;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class SocketImageProviderExample extends KinesisVideoCommon {

    private final AmazonKinesisVideo amazonKinesisVideo;
    private final Regions region;
    private final FrameVisitor frameVisitor;
    private PutMediaWorker putMediaWorker;
    private final InputStream inputStream;

    @Builder
    private SocketImageProviderExample(Regions region,
                                       String streamName,
                                       AWSCredentialsProvider credentialsProvider,
                                       OutputStream outputStream,
                                       InputStream inputStream) {
        super(region, credentialsProvider, streamName);

        final AmazonKinesisVideoClientBuilder builder = AmazonKinesisVideoClientBuilder.standard()
            .withCredentials(getCredentialsProvider())
            .withRegion(region);
        configureClient(builder);
        this.amazonKinesisVideo = builder.build();
        this.region = region;
        SocketFrameWriter kinesisVideoFrameViewer = new SocketFrameWriter(outputStream);
        this.frameVisitor = FrameVisitor.create(H264FrameReader.create(kinesisVideoFrameViewer));
        this.inputStream = inputStream;
    }

    public void execute() throws InterruptedException, IOException {

        log.info("Create stream (if does not exist) in region {} with name {}.", region, streamName);

        StreamOps streamOps = new StreamOps(region,  streamName, getCredentialsProvider());

        streamOps.recreateStreamIfNecessary();

        log.info("Start PutMedia to add data");

        ExecutorService executorService = Executors.newFixedThreadPool(1);

        //Start a PutMedia worker to write data to a Kinesis Video Stream.
        putMediaWorker = PutMediaWorker.create(getRegion(),
            getCredentialsProvider(),
            getStreamName(),
            inputStream,
            amazonKinesisVideo);

        // Start PutMedia to write data.
        executorService.submit(putMediaWorker);

        // Give time for put media to succeed.
        Thread.sleep(Seconds.seconds(2).toStandardDuration().getMillis());

        log.info("Calling GetDataEndpoint");
        String endPoint = amazonKinesisVideo.getDataEndpoint(
            new GetDataEndpointRequest()
                .withAPIName(APIName.GET_MEDIA)
                .withStreamName(streamOps.getStreamName())).getDataEndpoint();

        log.info("Constructing Client ");
        AmazonKinesisVideoMediaClientBuilder builder = AmazonKinesisVideoMediaClientBuilder.standard()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endPoint, region.getName()))
            .withCredentials(getCredentialsProvider());

        final AmazonKinesisVideoMedia videoMedia = builder.build();
        log.info("Calling GetMedia API");

        GetMediaResult result = videoMedia.getMedia(
            new GetMediaRequest()
                .withStreamName(streamName)
                .withStartSelector(new StartSelector().withStartSelectorType(StartSelectorType.EARLIEST))
        );

        StreamingMkvReader mkvStreamReader = StreamingMkvReader.createDefault(
            new InputStreamParserByteSource(result.getPayload())
        );

        log.info("StreamingMkvReader created for stream {} ", streamName);
        try {
            mkvStreamReader.apply(frameVisitor);
        } catch (MkvElementVisitException e) {
            log.error("Exception while accepting visitor {}", e);
        }

        log.info(" ----- Done ---");

        //Wait for the workers to finish.
        executorService.shutdown();
        executorService.awaitTermination(120, TimeUnit.SECONDS);
        if (!executorService.isTerminated()) {
            log.warn("Shutting down executor service by force");
            executorService.shutdownNow();
        } else {
            log.info("Executor service is shutdown");
        }
    }

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[0]);
        String streamName = args[1];
        String region = args[2];

        // Open socket
        Socket socket = new Socket("localhost",port);

        BufferedReader stdIn =new BufferedReader(new InputStreamReader(socket.getInputStream()));
        String in = stdIn.readLine();
        log.info("Socket input {}.", in);

        SocketImageProviderExample example = SocketImageProviderExample.builder()
            .region(Regions.fromName(region))
            .streamName(streamName)
            .credentialsProvider(new DefaultAWSCredentialsProviderChain())
            .outputStream(socket.getOutputStream())
            .inputStream(TestResourceUtil.getTestInputStream("clusters.mkv"))
            .build();

        example.execute();
    }
}