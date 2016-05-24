package goseaweed

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io/ioutil"
	"sort"
)

type ChunkInfo struct {
	Fid    string `json:"fid"`
	Offset int64  `json:"offset"`
	Size   int64  `json:"size"`
}

type ChunkList []*ChunkInfo

type ChunkManifest struct {
	Name   string    `json:"name,omitempty"`
	Mime   string    `json:"mime,omitempty"`
	Size   int64     `json:"size,omitempty"`
	Chunks ChunkList `json:"chunks,omitempty"`
}

func (s ChunkList) Len() int           { return len(s) }
func (s ChunkList) Less(i, j int) bool { return s[i].Offset < s[j].Offset }
func (s ChunkList) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func UnGzipData(input []byte) ([]byte, error) {
	buf := bytes.NewBuffer(input)
	r, _ := gzip.NewReader(buf)
	defer r.Close()
	output, err := ioutil.ReadAll(r)
	return output, err
}

func LoadChunkManifest(buffer []byte, isGzipped bool) (*ChunkManifest, error) {
	if isGzipped {
		var err error
		if buffer, err = UnGzipData(buffer); err != nil {
			return nil, err
		}
	}
	cm := ChunkManifest{}
	if e := json.Unmarshal(buffer, &cm); e != nil {
		return nil, e
	}
	sort.Sort(cm.Chunks)
	return &cm, nil
}

func (cm *ChunkManifest) Marshal() ([]byte, error) {
	return json.Marshal(cm)
}

func (sw *Seaweed) DeleteChunks(cm *ChunkManifest, collection string) error {
	deleteError := 0
	for _, ci := range cm.Chunks {
		if e := sw.DeleteFile(ci.Fid, collection); e != nil {
			deleteError++
		}
	}
	if deleteError > 0 {
		return errors.New("Not all chunks deleted.")
	}
	return nil
}
