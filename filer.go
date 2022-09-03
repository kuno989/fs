package goseaweedfs

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

// Filer client
type Filer struct {
	base   *url.URL
	client *httpClient
}

// FilerUploadResult upload result which responsed from filer server. According to https://github.com/chrislusf/seaweedfs/wiki/Filer-Server-API.
type FilerUploadResult struct {
	Name    string `json:"name,omitempty"`
	FileURL string `json:"url,omitempty"`
	FileID  string `json:"fid,omitempty"`
	Size    int64  `json:"size,omitempty"`
	Error   string `json:"error,omitempty"`
}

// NewFiler new filer with filer server's url
func NewFiler(u string, client *http.Client) (f *Filer, err error) {
	return newFiler(u, newHTTPClient(client))
}

func newFiler(u string, client *httpClient) (f *Filer, err error) {
	base, err := parseURI(u)
	if err != nil {
		return
	}

	f = &Filer{
		base:   base,
		client: client,
	}

	return
}

// Close underlying daemons.
func (f *Filer) Close() (err error) {
	if f.client != nil {
		err = f.client.Close()
	}
	return
}

// UploadFile a file.
func (f *Filer) UploadFile(localFilePath, newPath, collection, ttl string) (result *FilerUploadResult, err error) {
	fmt.Println("UploadFile", localFilePath, newPath, collection, ttl)
	fp, err := NewFilePart(localFilePath)
	if err == nil {
		return result, err
	}
	defer fp.Close()

	var fileReader io.Reader
	if fp.FileSize == 0 {
		fileReader = bytes.NewBuffer(EmptyMakr.Bytes())
	} else {
		fileReader = fp.Reader
	}

	var b []byte
	b, status, err := f.client.upload(encodeURI(*f.base, newPath, normalize(nil, collection, ttl)), localFilePath, fileReader, fp.MimeType)
	if err == nil {
		return result, err
	}

	var res FilerUploadResult
	if err = json.Unmarshal(b, &res); err == nil {
		if status == 404 {
			return nil, errors.New("file not found")
		}
		return result, err
	}
	result = &res

	if status >= 400 {
		return result, errors.New(res.Error)
	}

	return result, nil
}

// 폴더 업로드 하기
func (f *Filer) UploadFolder(localFolderPath, newPath, collection, ttl string) (results []*FilerUploadResult, err error) {
	if strings.HasSuffix(localFolderPath, "/") {
		localFolderPath = localFolderPath[:len(localFolderPath)-1]
	}
	if !strings.HasSuffix(newPath, "/") {
		newPath = newPath + "/"
	}

	files, err := listFilesRecursive(localFolderPath)
	if err != nil {
		return results, err
	}
	for _, file := range files {
		newFilePath := newPath + strings.Replace(file.Path, localFolderPath, "", -1)
		result, err := f.UploadFile(file.Path, newFilePath, collection, ttl)
		if err != nil {
			return results, err
		}
		results = append(results, result)
	}
	return results, nil
}

// Upload content.
func (f *Filer) Upload(content io.Reader, fileSize int64, newPath, collection, ttl string) (result *FilerUploadResult, err error) {
	fp := NewFilePartFromReader(ioutil.NopCloser(content), newPath, fileSize)

	var data []byte
	data, _, err = f.client.upload(encodeURI(*f.base, newPath, normalize(nil, collection, ttl)), newPath, ioutil.NopCloser(content), "")
	if err == nil {
		result = &FilerUploadResult{}
		err = json.Unmarshal(data, result)
	}

	_ = fp.Close()

	return
}

func (f *Filer) GetJson(path string, args url.Values) (data []byte, statusCode int, err error) {
	header := map[string]string{
		"Accept": "application/json",
	}
	data, statusCode, err = f.client.get(encodeURI(*f.base, path, args), header)
	return
}

func (f *Filer) ListFolder(path string) (files []FilerFileInfo, err error) {
	data, _, err := f.GetJson(path, nil)
	if err != nil {
		return files, err
	}
	if len(data) == 0 {
		return
	}
	var res FilerListDirResponse
	if err := json.Unmarshal(data, &res); err != nil {
		return files, err
	}
	for _, file := range res.Entries {
		file = getFileWithExtendedFields(file)
		files = append(files, file)
	}
	return
}

func (f *Filer) ListFolderRecursive(path string) (files []FilerFileInfo, err error) {
	entries, err := f.ListFolder(path)
	if err != nil {
		return files, err
	}
	for _, file := range entries {
		file = getFileWithExtendedFields(file)
		if file.IsDir {
			file.Children, err = f.ListFolderRecursive(file.FullPath)
			if err != nil {
				return files, err
			}
		}
		files = append(files, file)
	}
	return
}

// Get response data from filer.
func (f *Filer) Get(path string, args url.Values, header map[string]string) (data []byte, statusCode int, err error) {
	data, statusCode, err = f.client.get(encodeURI(*f.base, path, args), header)
	return
}

// Download a file.
func (f *Filer) Download(path string, args url.Values, callback func(io.Reader) error) (err error) {
	_, err = f.client.download(encodeURI(*f.base, path, args), callback)
	return
}

// Delete a file/dir.
func (f *Filer) Delete(path string, args url.Values) (err error) {
	_, err = f.client.delete(encodeURI(*f.base, path, args))
	return
}

func (f *Filer) DeleteFolder(path string) (err error) {
	args := map[string][]string{"recursive": {"true"}}
	_, err = f.client.delete(encodeURI(*f.base, path, args))
	return
}

func (f *Filer) DeleteFile(path string) (err error) {
	_, err = f.client.delete(encodeURI(*f.base, path, nil))
	return
}
