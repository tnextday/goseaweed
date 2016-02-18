package goseaweed

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"path/filepath"
	"strings"
)

type HttpClient struct {
	Client *http.Client
}

type UploadResult struct {
	Name  string `json:"name,omitempty"`
	Size  int64  `json:"size,omitempty"`
	Error string `json:"error,omitempty"`
}

var fileNameEscaper = strings.NewReplacer("\\", "\\\\", "\"", "\\\"")

func NewHttpClient(MaxIdleConnsPerHost int) *HttpClient {
	Transport := &http.Transport{
		MaxIdleConnsPerHost: 1024,
	}
	c := &http.Client{Transport: Transport}
	return &HttpClient{Client: c}
}

func MkUrl(host, path string, args url.Values) string {
	u := url.URL{
		Scheme: "http",
		Host:   host,
		Path:   path,
	}
	if args != nil {
		u.RawQuery = args.Encode()
	}
	return u.String()
}

func (hc *HttpClient) PostBytes(url string, body []byte) ([]byte, error) {
	r, err := hc.Client.Post(url, "application/octet-stream", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("Post to %s: %v", url, err)
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("Read response body: %v", err)
	}
	return b, nil
}

func (hc *HttpClient) PostEx(host, path string, values url.Values) (content []byte, statusCode int, e error) {
	url := MkUrl(host, path, nil)
	//glog.V(4).Infoln("Post", url+"?"+values.Encode())
	r, err := hc.Client.PostForm(url, values)
	if err != nil {
		return nil, 0, err
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, r.StatusCode, err
	}
	return b, r.StatusCode, nil
}

func (hc *HttpClient) Post(host, path string, values url.Values) (content []byte, e error) {
	content, _, e = hc.PostEx(host, path, values)
	return
}

func (hc *HttpClient) Get(host, path string, values url.Values) ([]byte, error) {
	url := MkUrl(host, path, values)
	r, err := hc.Client.Get(url)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if r.StatusCode != 200 {
		return nil, fmt.Errorf("%s: %s", url, r.Status)
	}
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (hc *HttpClient) Delete(url string) error {
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}
	resp, e := hc.Client.Do(req)
	if e != nil {
		return e
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	switch resp.StatusCode {
	case http.StatusNotFound, http.StatusAccepted, http.StatusOK:
		return nil
	}
	m := make(map[string]interface{})
	if e := json.Unmarshal(body, &m); e == nil {
		if s, ok := m["error"].(string); ok {
			return errors.New(s)
		}
	}
	return errors.New(string(body))
}

func (hc *HttpClient) DownloadUrl(fileUrl string) (filename string, rc io.ReadCloser, e error) {
	response, err := hc.Client.Get(fileUrl)
	if err != nil {
		return "", nil, err
	}
	if response.StatusCode != http.StatusOK {
		response.Body.Close()
		return "", nil, fmt.Errorf("%s: %s", fileUrl, response.Status)
	}
	contentDisposition := response.Header["Content-Disposition"]
	if len(contentDisposition) > 0 {
		if strings.HasPrefix(contentDisposition[0], "filename=") {
			filename = contentDisposition[0][len("filename="):]
			filename = strings.Trim(filename, "\"")
		}
	}
	rc = response.Body
	return
}

func (hc *HttpClient) Do(req *http.Request) (resp *http.Response, err error) {
	return hc.Client.Do(req)
}

func (hc *HttpClient) Upload(uploadUrl string, filename string, reader io.Reader, isGzipped bool, mtype string) (*UploadResult, error) {
	return hc.uploadContent(uploadUrl, func(w io.Writer) (err error) {
		_, err = io.Copy(w, reader)
		return
	}, filename, isGzipped, mtype)
}

func (hc *HttpClient) uploadContent(uploadUrl string, fillBufferFunction func(w io.Writer) error, filename string, isGzipped bool, mtype string) (*UploadResult, error) {
	body_buf := bytes.NewBufferString("")
	bodyWriter := multipart.NewWriter(body_buf)
	h := make(textproto.MIMEHeader)

	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename="%s"`, fileNameEscaper.Replace(filename)))
	if mtype == "" {
		mtype = mime.TypeByExtension(strings.ToLower(filepath.Ext(filename)))
	}
	if mtype != "" {
		h.Set("Content-Type", mtype)
	}
	if isGzipped {
		h.Set("Content-Encoding", "gzip")
	}

	file_writer, cp_err := bodyWriter.CreatePart(h)
	if cp_err != nil {
		//glog.V(0).Infoln("error creating form file", cp_err.Error())
		return nil, cp_err
	}
	if err := fillBufferFunction(file_writer); err != nil {
		//glog.V(0).Infoln("error copying data", err)
		return nil, err
	}
	content_type := bodyWriter.FormDataContentType()
	if err := bodyWriter.Close(); err != nil {
		//glog.V(0).Infoln("error closing body", err)
		return nil, err
	}
	resp, post_err := hc.Client.Post(uploadUrl, content_type, body_buf)
	if post_err != nil {
		//glog.V(0).Infoln("failing to upload to", uploadUrl, post_err.Error())
		return nil, post_err
	}
	defer resp.Body.Close()
	resp_body, ra_err := ioutil.ReadAll(resp.Body)
	if ra_err != nil {
		return nil, ra_err
	}
	var ret UploadResult
	unmarshal_err := json.Unmarshal(resp_body, &ret)
	if unmarshal_err != nil {
		//glog.V(0).Infoln("failing to read upload resonse", uploadUrl, string(resp_body))
		return nil, unmarshal_err
	}
	if ret.Error != "" {
		return nil, errors.New(ret.Error)
	}
	return &ret, nil
}
