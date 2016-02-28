package goseaweed

import "testing"

const (
	master = "192.168.199.2:9333"
)

var (
	weed = NewSeaweed(master)
)

func TestAssign(t *testing.T) {

}

func TestUploadFile(t *testing.T) {
	testFiles := []string{
		"chunk.go",
		"lookup.go",
		"test.zip",
	}
	weed.ChunkSize = 256 * 1024
	for _, f := range testFiles {
		fid, e := weed.UploadFile(f, "", "")
		if e != nil {
			t.Fatalf("Upload %s error, %v", f, e)
		}
		t.Logf("Upload %s -> %s", f, fid)
	}
}

func TestBatchUploadFiles(t *testing.T) {
	testFiles := []string{
		"chunk.go",
		"lookup.go",
		"submit.go",
		"seaweed.go",
	}
	rets, e := weed.BatchUploadFiles(testFiles, "", "")
	if e != nil {
		t.Fatal(e)
	}

	for _, r := range rets {
		if r.Error != "" {
			t.Fatalf("Upload %s failed, %s", r.FileName, r.Error)
		}
		t.Logf("Upload %s -> %s", r.FileName, r.Fid)

	}
}

func TestDeleteFiles(t *testing.T) {
	fids := []string{
		"2,05b209def1e5",
		"2,05b209def1e5_1",
		"2,05b209def1e5_2",
		"2,05b209def1e5_3",
	}
	for _, fid := range fids {
		t.Logf("Deleting %s", fid)
		if e := weed.DeleteFile(fid, ""); e != nil {
			t.Fatal(e)
		}
	}

}
