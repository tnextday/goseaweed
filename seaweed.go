package goseaweed

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
)

type Seaweed struct {
	Master    string
	HC        *HttpClient
	ChunkSize int64
	vc        VidCache 	// caching of volume locations, re-check if after 10 minutes
}

func NewSeaweed(master string) (sw *Seaweed) {
	return &Seaweed{
		Master: master,
		HC:     NewHttpClient(1024),
	}
}

type AssignResult struct {
	Fid       string `json:"fid,omitempty"`
	Url       string `json:"url,omitempty"`
	PublicUrl string `json:"publicUrl,omitempty"`
	Count     uint64 `json:"count,omitempty"`
	Error     string `json:"error,omitempty"`
}

func (sw *Seaweed) Assign(count int, collection string, ttl string) (*AssignResult, error) {
	values := make(url.Values)
	values.Set("count", strconv.Itoa(count))
	if collection != "" {
		values.Set("collection", collection)
	}
	if ttl != "" {
		values.Set("ttl", ttl)
	}
	jsonBlob, err := sw.HC.Post(sw.Master, "/dir/assign", values)
	if err != nil {
		return nil, err
	}
	var ret AssignResult
	err = json.Unmarshal(jsonBlob, &ret)
	if err != nil {
		return nil, fmt.Errorf("/dir/assign result JSON unmarshal error:%v, json:%s", err, string(jsonBlob))
	}
	if ret.Count <= 0 {
		return nil, errors.New(ret.Error)
	}
	return &ret, nil
}

func (sw *Seaweed) UploadFile(filePath, collection, ttl string) (fid string, err error) {
	fp, e := NewFilePart(filePath)
	if e != nil {
		return "", e
	}
	fp.Collection = collection
	fp.Ttl = ttl
	return sw.UploadFilePart(&fp)
}

func (sw *Seaweed) BatchUploadFiles(files []string,
	collection string, ttl string) ([]SubmitResult, error) {

	fps, e := NewFileParts(files)
	if e != nil {
		return nil, e
	}
	return sw.BatchUploadFileParts(fps, collection, ttl)
}

func (sw *Seaweed) ReplaceFile(fid, filePath string, deleteFirst bool) error {
	fp, e := NewFilePart(filePath)
	if e != nil {
		return e
	}
	fp.Fid = fid
	_, e = sw.ReplaceFilePart(&fp, deleteFirst)
	return e
}

func (sw *Seaweed) DeleteFile(fileId string) error {
	fileUrl, err := sw.LookupFileId(fileId, false)
	if err != nil {
		return fmt.Errorf("Failed to lookup %s:%v", fileId, err)
	}
	err = sw.HC.Delete(fileUrl)
	if err != nil {
		return fmt.Errorf("Failed to delete %s:%v", fileUrl, err)
	}
	return nil
}
