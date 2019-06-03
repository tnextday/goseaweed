package goseaweed

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"
)

type Seaweed struct {
	Master    string
	HC        *HttpClient
	ChunkSize int64
	vc        VidCache // caching of volume locations, re-check if after 10 minutes
}

func NewSeaweed(master string) (sw *Seaweed) {
	return &Seaweed{
		Master: master,
		HC:     NewHttpClient(512, 45*time.Second),
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

func (sw *Seaweed) DeleteFile(fileId, collection string) error {
	fileUrl, err := sw.LookupFileId(fileId, collection, false)
	if err != nil {
		return fmt.Errorf("Failed to lookup %s:%v", fileId, err)
	}
	err = sw.HC.Delete(fileUrl)
	if err != nil {
		return fmt.Errorf("Failed to delete %s:%v", fileUrl, err)
	}
	return nil
}

//Todo: grow volume add by andy
func (sw *Seaweed) Grow(count int, collection, replication, dataCenter, ttl string) error {
	args := make(url.Values)
	if count > 0 {
		args.Set("count", strconv.Itoa(count))
	}
	if collection != "" {
		args.Set("collection", collection)
	}
	if replication != "" {
		args.Set("replication", replication)
	}
	if dataCenter != "" {
		args.Set("dataCenter", dataCenter)
	}
	if ttl != "" {
		args.Set("ttl", ttl)
	}

	return sw.GrowArgs(args)
}

//Todo: grow volume add by andy
func (sw *Seaweed) GrowArgs(args url.Values) error {
	_, err := sw.HC.Get(sw.Master, "/vol/grow", args)
	if err == nil {
		fmt.Println("grow volume success")
		return nil
	}
	return err
}
