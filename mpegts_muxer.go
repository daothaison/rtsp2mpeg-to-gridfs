package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/bluenviron/mediacommon/pkg/formats/mpegts"
)

func durationGoToMPEGTS(v time.Duration) int64 {
	return int64(v.Seconds() * 10000)
}

// mpegtsMuxer allows to save a MPEG1-audio stream into a MPEG-TS file.
type mpegtsMuxer struct {
	f     *os.File
	b     *bufio.Writer
	w     *mpegts.Writer
	track *mpegts.Track
}

// newMPEGTSMuxer allocates a mpegtsMuxer.
func newMPEGTSMuxer(index uint32) (*mpegtsMuxer, error) {
	//f, err := os.CreateTemp("", fmt.Sprintf("stream_%d.ts", index))
	f, err := os.Create(fmt.Sprintf("stream_%d.ts", index))
	if err != nil {
		return nil, err
	}
	b := bufio.NewWriter(f)

	track := &mpegts.Track{
		Codec: &mpegts.CodecMPEG1Audio{},
	}

	w := mpegts.NewWriter(b, []*mpegts.Track{track})

	return &mpegtsMuxer{
		f:     f,
		b:     b,
		w:     w,
		track: track,
	}, nil
}

func (e *mpegtsMuxer) GetFile() *os.File {
	return e.f
}

// close closes all the mpegtsMuxer resources.
func (e *mpegtsMuxer) close() {
	e.b.Flush()
	e.f.Close()
}

// encode encodes MPEG-1 audio access units into MPEG-TS.
func (e *mpegtsMuxer) encode(au []byte, pts time.Duration) error {
	// encode into MPEG-TS
	err := e.w.WriteMPEG1Audio(e.track, durationGoToMPEGTS(pts), [][]byte{au})
	if err != nil {
		return err
	}

	log.Println("wrote TS packet")
	return nil
}
