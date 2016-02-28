package goseaweed

import (
	"bytes"
	"io"
	"mime"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
)

type FilePart struct {
	Reader     io.Reader
	FileName   string
	FileSize   int64
	IsGzipped  bool
	MimeType   string
	ModTime    int64 //in seconds
	Collection string
	Ttl        string
	Server     string
	Fid        string
}

type SubmitResult struct {
	FileName string `json:"fileName,omitempty"`
	FileUrl  string `json:"fileUrl,omitempty"`
	Fid      string `json:"fid,omitempty"`
	Size     int64  `json:"size,omitempty"`
	Error    string `json:"error,omitempty"`
}

func (sw *Seaweed) BatchUploadFileParts(files []FilePart,
	collection string, ttl string) ([]SubmitResult, error) {
	results := make([]SubmitResult, len(files))
	for index, file := range files {
		results[index].FileName = file.FileName
	}
	ret, err := sw.Assign(len(files), collection, ttl)
	if err != nil {
		for index := range files {
			results[index].Error = err.Error()
		}
		return results, err
	}
	for index, file := range files {
		file.Fid = ret.Fid
		if index > 0 {
			file.Fid = file.Fid + "_" + strconv.Itoa(index)
		}
		file.Server = ret.Url
		file.Collection = collection
		_, err = sw.UploadFilePart(&file)
		results[index].Size = file.FileSize
		if err != nil {
			results[index].Error = err.Error()
		}
		results[index].Fid = file.Fid
		results[index].FileUrl = ret.PublicUrl + "/" + file.Fid
	}
	return results, nil
}

func (sw *Seaweed) UploadFilePart(fp *FilePart) (fid string, err error) {
	if fp.Fid == "" {
		ret, err := sw.Assign(1, fp.Collection, fp.Ttl)
		if err != nil {
			return "", err
		}
		fp.Server, fp.Fid = ret.Url, ret.Fid
	}
	if fp.Server == "" {
		if fp.Server, err = sw.LookupFileId(fp.Fid, fp.Collection, false); err != nil {
			return
		}
	}

	if closer, ok := fp.Reader.(io.Closer); ok {
		defer closer.Close()
	}
	baseName := path.Base(fp.FileName)
	if sw.ChunkSize > 0 && fp.FileSize > sw.ChunkSize {
		chunks := fp.FileSize/sw.ChunkSize + 1
		cm := ChunkManifest{
			Name:   baseName,
			Size:   fp.FileSize,
			Mime:   fp.MimeType,
			Chunks: make([]*ChunkInfo, 0, chunks),
		}

		for i := int64(0); i < chunks; i++ {
			id, count, e := sw.uploadChunk(fp, baseName+"-"+strconv.FormatInt(i+1, 10))
			if e != nil {
				// delete all uploaded chunks
				sw.DeleteChunks(&cm, fp.Collection)
				return "", e
			}
			cm.Chunks = append(cm.Chunks,
				&ChunkInfo{
					Offset: i * sw.ChunkSize,
					Size:   int64(count),
					Fid:    id,
				},
			)
		}
		err = sw.uploadManifest(fp, &cm)
		if err != nil {
			// delete all uploaded chunks
			sw.DeleteChunks(&cm, fp.Collection)
		}
	} else {
		args := url.Values{}
		if fp.ModTime != 0 {
			args.Set("ts", strconv.FormatInt(fp.ModTime, 10))
		}
		fileUrl := MkUrl(fp.Server, fp.Fid, args)
		_, err = sw.HC.Upload(fileUrl, baseName, fp.Reader, fp.IsGzipped, fp.MimeType)
	}
	if err == nil {
		fid = fp.Fid
	}
	return
}

func (sw *Seaweed) ReplaceFilePart(fp *FilePart, deleteFirst bool) (fid string, err error) {
	if deleteFirst && fp.Fid != "" {
		sw.DeleteFile(fp.Fid, fp.Collection)
	}
	return sw.UploadFilePart(fp)
}

func (sw *Seaweed) uploadChunk(fp *FilePart, filename string) (fid string, size int64, e error) {
	ret, err := sw.Assign(1, fp.Collection, fp.Ttl)
	if err != nil {
		return "", 0, err
	}

	fileUrl, fid := MkUrl(ret.Url, ret.Fid, nil), ret.Fid
	//glog.V(4).Info("Uploading part ", filename, " to ", fileUrl, "...")
	reader := io.LimitReader(fp.Reader, sw.ChunkSize)
	uploadResult, uploadError := sw.HC.Upload(fileUrl, filename, reader, false, "application/octet-stream")
	if uploadError != nil {
		return fid, 0, uploadError
	}
	return fid, uploadResult.Size, nil
}

func (sw *Seaweed) uploadManifest(fp *FilePart, manifest *ChunkManifest) error {
	buf, e := manifest.Marshal()
	if e != nil {
		return e
	}
	bufReader := bytes.NewReader(buf)
	//glog.V(4).Info("Uploading chunks manifest ", manifest.Name, " to ", fileUrl, "...")
	args := url.Values{}
	if fp.ModTime != 0 {
		args.Set("ts", strconv.FormatInt(fp.ModTime, 10))
	}
	args.Set("cm", "true")
	u := MkUrl(fp.Server, fp.Fid, args)
	_, e = sw.HC.Upload(u, manifest.Name, bufReader, false, "application/json")
	return e
}

func NewFilePart(fullPathFilename string) (ret FilePart, err error) {
	fh, openErr := os.Open(fullPathFilename)
	if openErr != nil {
		//glog.V(0).Info("Failed to open file: ", fullPathFilename)
		return ret, openErr
	}
	ret.Reader = fh

	if fi, fiErr := fh.Stat(); fiErr != nil {
		//glog.V(0).Info("Failed to stat file:", fullPathFilename)
		return ret, fiErr
	} else {
		ret.ModTime = fi.ModTime().UTC().Unix()
		ret.FileSize = fi.Size()
	}
	ext := strings.ToLower(path.Ext(fullPathFilename))
	ret.IsGzipped = ext == ".gz"
	if ret.IsGzipped {
		ret.FileName = fullPathFilename[0 : len(fullPathFilename)-3]
	}
	ret.FileName = fullPathFilename
	if ext != "" {
		ret.MimeType = mime.TypeByExtension(ext)
	}

	return ret, nil
}

func NewFileParts(fullPathFilenames []string) (ret []FilePart, err error) {
	ret = make([]FilePart, len(fullPathFilenames))
	for index, file := range fullPathFilenames {
		if ret[index], err = NewFilePart(file); err != nil {
			return
		}
	}
	return
}
