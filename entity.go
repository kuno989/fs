package goseaweedfs

import "time"

type FileInfo struct {
	Name string `json:"name"`
	Path string `json:"path"`
	Md5  string `json:"md5"`
}

type FilerListDirResponse struct {
	Path    string
	Entries []FilerFileInfo
}

type FilerFileInfo struct {
	FullPath        string
	Mtime           time.Time
	Crtime          time.Time
	Mode            int
	Uid             int
	Gid             int
	Mime            string
	Replication     string
	Collection      string
	TtlSec          int
	UserName        string
	GroupNames      string
	SymlinkTarget   string
	Md5             string
	FileSize        int64
	Extended        string
	HardLinkId      string
	HardLinkCounter int64
	Chunks          interface{}
	Children        []FilerFileInfo
	Name            string
	Extension       string
	IsDir           bool
}
