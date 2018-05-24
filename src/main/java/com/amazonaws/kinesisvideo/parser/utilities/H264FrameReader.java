/*
Copyright 2017-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at

   http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file.
This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
*/
package com.amazonaws.kinesisvideo.parser.utilities;

import com.amazonaws.kinesisvideo.parser.mkv.Frame;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jcodec.codecs.h264.H264Decoder;
import org.jcodec.codecs.h264.mp4.AvcCBox;
import org.jcodec.common.model.ColorSpace;
import org.jcodec.common.model.Picture;
import org.jcodec.scale.AWTUtil;
import org.jcodec.scale.Transform;
import org.jcodec.scale.Yuv420jToRgb;

import java.awt.image.BufferedImage;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

import static org.jcodec.codecs.h264.H264Utils.splitMOVPacket;

@Slf4j
public class H264FrameReader implements FrameVisitor.FrameProcessor {

    private final SocketFrameWriter kinesisVideoFrameViewer;
    private final H264Decoder decoder = new H264Decoder();
    private final Transform transform = new Yuv420jToRgb();

    @Getter
    private int frameCount;

    private byte[] codecPrivateData;

    protected H264FrameReader(final SocketFrameWriter kinesisVideoFrameViewer) {
        this.kinesisVideoFrameViewer = kinesisVideoFrameViewer;
    }

    public static H264FrameReader create(SocketFrameWriter kinesisVideoFrameViewer) {
        return new H264FrameReader(kinesisVideoFrameViewer);
    }

    @Override
    public void process(Frame frame, MkvTrackMetadata trackMetadata, Optional<FragmentMetadata> fragmentMetadata) {
        final BufferedImage bufferedImage = decodeH264Frame(frame, trackMetadata);
        kinesisVideoFrameViewer.update(bufferedImage, frame, fragmentMetadata, trackMetadata);
    }

    protected BufferedImage decodeH264Frame(Frame frame, MkvTrackMetadata trackMetadata) {
        ByteBuffer frameBuffer = frame.getFrameData();
        int pixelWidth = trackMetadata.getPixelWidth().get().intValue();
        int pixelHeight = trackMetadata.getPixelHeight().get().intValue();
        codecPrivateData = trackMetadata.getCodecPrivateData().array();
        log.debug("Decoding frames ... ");
        // Read the bytes that appear to comprise the header
        // See: https://www.matroska.org/technical/specs/index.html#simpleblock_structure

        Picture rgb = Picture.create(pixelWidth, pixelHeight, ColorSpace.RGB);
        BufferedImage bufferedImage = new BufferedImage(pixelWidth, pixelHeight, BufferedImage.TYPE_3BYTE_BGR);
        AvcCBox avcC = AvcCBox.parseAvcCBox(ByteBuffer.wrap(codecPrivateData));

        decoder.addSps(avcC.getSpsList());
        decoder.addPps(avcC.getPpsList());

        Picture buf = Picture.create(pixelWidth, pixelHeight, ColorSpace.YUV420J);
        List<ByteBuffer> byteBuffers = splitMOVPacket(frameBuffer, avcC);
        Picture pic = decoder.decodeFrameFromNals(byteBuffers, buf.getData());

        if (pic != null) {
            // Work around for color issues in JCodec
            // https://github.com/jcodec/jcodec/issues/59
            // https://github.com/jcodec/jcodec/issues/192
            byte[][] dataTemp = new byte[3][pic.getData().length];
            dataTemp[0] = pic.getPlaneData(0);
            dataTemp[1] = pic.getPlaneData(2);
            dataTemp[2] = pic.getPlaneData(1);

            Picture tmpBuf = Picture.createPicture(pixelWidth, pixelHeight, dataTemp, ColorSpace.YUV420J);
            transform.transform(tmpBuf, rgb);
            AWTUtil.toBufferedImage(rgb, bufferedImage);
            frameCount++;
        }
        return bufferedImage;
    }
}
