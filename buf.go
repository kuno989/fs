package goseaweedfs

type FileMark rune

func (m FileMark) String() string {
	return string(m)
}

func (m FileMark) Bytes() []byte {
	return []byte(m.String())
}

func IsFileMarkBytes(b []byte, m FileMark) bool {
	return string(b) == m.String()
}
