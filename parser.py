import socket
import base64
from PIL import Image
import StringIO
import subprocess

HOST = "localhost"
PORT = 2009
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print('Socket created')

try:
    s.bind((HOST, PORT))
except socket.error as err:
    print('Bind failed. Error Code : ' .format(err))

s.listen(10)
# Run java process to listen to the data
p = subprocess.Popen(['java',
                      '-cp',
                      'target/amazon-kinesis-video-streams-parser-library-1.0.5-SNAPSHOT.jar',
                      'com.amazonaws.kinesisvideo.parser.examples.SocketImageProviderExample',
                      '2009', # port
                      'josvijay-demo-android', # stream name
                      'us-west-2', # region
                      ]
                     )
print("Socket Listening")
conn, addr = s.accept()
conn.send(bytes("Message"+"\r\n"))
print("Message sent")

image_data = ""
while(True):
    data = conn.recv(1024)
    image_data += data.decode(encoding='UTF-8')
    if "\n" in image_data:
      data = image_data.split("\n")
      tempBuff = StringIO.StringIO()
      tempBuff.write(data[0].decode('base64'))
      tempBuff.seek(0) #need to jump back to the beginning before handing it off to PIL
      image = Image.open(tempBuff)
      image.show()
      image.close()
      image_data = data[1]
