package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/bluenviron/gortsplib/v3"
	"github.com/bluenviron/gortsplib/v3/pkg/formats"
	"github.com/bluenviron/gortsplib/v3/pkg/url"
	"github.com/grafov/m3u8"
	"github.com/pion/rtp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

func readStream(streamUrl string) {
	c := gortsplib.Client{}

	// parse URL
	u, err := url.Parse(streamUrl)
	if err != nil {
		panic(err)
	}

	// connect to the server
	err = c.Start(u.Scheme, u.Host)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	// find available medias
	medias, baseURL, _, err := c.Describe(u)
	if err != nil {
		panic(err)
	}

	// find the MPEG-1 audio media and format
	var forma *formats.MPEG1Audio
	medi := medias.FindFormat(&forma)
	if medi == nil {
		panic("media not found")
	}

	// setup RTP/MPEG-1 audio -> MPEG-1 audio decoder
	rtpDec, err := forma.CreateDecoder2()
	if err != nil {
		panic(err)
	}

	var index uint32 = 0
	// setup MPEG-1 audio -> MPEG-TS muxer
	mpegtsMuxer, err := newMPEGTSMuxer(index)
	if err != nil {
		panic(err)
	}

	// setup a single media
	_, err = c.Setup(medi, baseURL, 0, 0)
	if err != nil {
		panic(err)
	}

	// create a m3u8 playlist
	playlist, err := m3u8.NewMediaPlaylist(0, 100000000)

	startTime := time.Now()
	// called when a RTP packet arrives
	c.OnPacketRTP(medi, forma, func(pkt *rtp.Packet) {
		// after startTime + 10 seconds, save the content into a new file
		if time.Since(startTime) > 10*time.Second {
			mpegtsMuxer.close()

			storeIntoGridFs(mpegtsMuxer.GetFile().Name(), fmt.Sprintf("stream_%d.ts", index))

			if err != nil {
				fmt.Printf("Create playlist err: %s\n", err)
			}
			_ = playlist.Append(fmt.Sprintf("stream_%d.ts", index), 10, "")
			playlist.Close()
			fileId := findGridFsFile("recordingInfo.Id", "stream_id.m3u8")
			storeM3u8IntoGridFs(playlist.Encode(), "stream_id.m3u8", fileId)

			index++
			startTime = time.Now()

			mpegtsMuxer, err = newMPEGTSMuxer(index)
			if err != nil {
				panic(err)
			}
		}

		// decode timestamp
		//pts, ok := c.PacketPTS(medi, pkt)
		//if !ok {
		//	log.Printf("waiting for timestamp")
		//	return
		//}

		// extract access units from RTP packets
		aus, pts, err := rtpDec.Decode(pkt)
		if err != nil {
			log.Printf("ERR: %v", err)
			return
		}

		for _, au := range aus {
			// encode the access unit into MPEG-TS
			err = mpegtsMuxer.encode(au, pts)
			if err != nil {
				log.Printf("ERR: %v", err)
				return
			}
		}

		// encode access units into MPEG-TS
		log.Printf("Saved TS packet for package at time %v", index)
	})

	// start playing
	_, err = c.Play(nil)
	if err != nil {
		panic(err)
	}

	// wait until a fatal error
	panic(c.Wait())
}

var streamMutex sync.Mutex
var client *mongo.Client
var db *mongo.Database

func storeIntoGridFs(fileTs string, savedFilename string) {
	fs, err := gridfs.NewBucket(
		db,
	)
	if err != nil {
		log.Printf("Error creating GridFS bucket: %s \n", err)
		return
	}

	streamMutex.Lock()
	defer streamMutex.Unlock()
	file, err := os.Open(fileTs)
	if err != nil {
		log.Printf("Error opening HLS file: %s\n", err)
		return
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Printf("Error closing TS file: %s\n", err)
		}
	}(file)

	log.Printf("Uploading ts file %s to GridFS\n", fileTs)

	uploadOpts := options.GridFSUpload().SetMetadata(bson.D{{"stream_id", "recordingInfo.Id"}})
	uploadStream, err := fs.OpenUploadStream(savedFilename, uploadOpts)
	if err != nil {
		log.Printf("Error creating GridFS upload stream: %s\n", err)
		return
	}
	defer uploadStream.Close()

	fileContent, err := io.ReadAll(file)
	if err != nil {
		log.Printf("Error reading file content from upload stream: %s \n", err)
	}

	_, err = uploadStream.Write(fileContent)
	if err != nil {
		log.Printf("Error writing to GridFS: %s\n", err)
		return
	}

	fmt.Printf("File uploaded to GridFS for stream ID: %s, file id: %s \n", "recordingInfo.Id", uploadStream.FileID)
}

func storeM3u8IntoGridFs(m3u8Content *bytes.Buffer, savedFilename string, fileId string) {
	fs, err := gridfs.NewBucket(
		db,
	)
	if err != nil {
		log.Printf("Error creating GridFS bucket: %s \n", err)
		return
	}

	if fileId != "" {
		log.Printf("Drop exist segment %s in GridFS\n", savedFilename)
		id, _ := primitive.ObjectIDFromHex(fileId)
		if err := fs.Delete(id); err != nil {
			log.Printf("Drop exist m3u8 file %s in GridFS error: %s\n", savedFilename, err.Error())
		}
	}

	streamMutex.Lock()
	defer streamMutex.Unlock()
	log.Printf("Uploading file %s to GridFS\n", m3u8Content.String())

	uploadOpts := options.GridFSUpload().SetMetadata(bson.D{{"stream_id", "recordingInfo.Id"}})
	uploadStream, err := fs.UploadFromStream(savedFilename, m3u8Content, uploadOpts)
	if err != nil {
		log.Printf("Error creating GridFS file content: %s\n", err)
		return
	}

	fmt.Printf("File uploaded to GridFS for stream ID: %s, file id: %s \n", "recordingInfo.Id", uploadStream)
}

func findGridFsFile(streamId string, segmentUri string) string {
	type gridfsFile struct {
		Id string `bson:"_id"`
	}
	var file gridfsFile

	// find one record in mongo with filter
	filter := bson.D{{"filename", segmentUri}, {"metadata", bson.D{{"stream_id", streamId}}}}
	err := db.Collection("fs.files").FindOne(context.Background(), filter).Decode(&file)
	if err != nil {
		return ""
	}

	return file.Id
}

// This example shows how to
// 1. connect to a RTSP server
// 2. check if there's an MPEG-1 audio media
// 3. save the content of the media into a file in MPEG-TS format

func main() {
	clientOptions := options.Client().ApplyURI("mongodb://mongoadmin:mongoadmin@localhost:27017")
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(context.Background())

	db = client.Database("rstp")

	readStream("rtsp://localhost:8554/live")
}
