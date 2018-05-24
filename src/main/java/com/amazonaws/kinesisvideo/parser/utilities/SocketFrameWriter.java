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
import com.amazonaws.util.Base64;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;

public class SocketFrameWriter {
    final OutputStream outputStream;

    public SocketFrameWriter(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    public void update(final BufferedImage image,
                       final Frame frame,
                       final Optional<FragmentMetadata> fragmentMetadata,
                       final MkvTrackMetadata trackMetadata) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ImageIO.write(image, "jpg", baos );
            baos.flush();
            outputStream.write(Base64.encode(baos.toByteArray()));
            outputStream.write("$".getBytes());
            outputStream.write(Base64.encode(Integer.toString(frame.getTimeCode()).getBytes()));
            outputStream.write("$".getBytes());
            outputStream.write(Base64.encode(fragmentMetadata.get().toString().getBytes()));
            outputStream.write("\n".getBytes());
            outputStream.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

