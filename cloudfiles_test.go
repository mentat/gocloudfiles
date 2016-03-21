package gocloudfiles

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

var (
	TestUserName = os.Getenv("TEST_USERNAME")
	TestApiKey   = os.Getenv("TEST_KEY")
)

func TestMain(m *testing.M) {
	if TestUserName == "" || TestApiKey == "" {
		fmt.Println("Please set the environment variables TEST_USERNAME and TEST_KEY")
		os.Exit(1)
	} else {
		os.Exit(m.Run())
	}
}

func TestGetFileLength(t *testing.T) {
	// Test we can get the length of a cloudfiles file without pulling the entire file
	fmt.Println("Test get file length...")
	cf := NewCloudFiles(TestUserName, TestApiKey)
	err := cf.Authorize()

	if err != nil {
		t.Fatalf("Could not authorize: %s", err)
	}

	size, _, err := cf.GetFileSize("IAD", "testing", "ubuntu-14.04.4-desktop-amd64.iso")
	if err != nil {
		t.Fatalf("Could not get file size: %s", err)
	}

	realFileSize := int64(1069547520)

	if size != realFileSize {
		t.Fatalf("Size should be %d but instead is %d.", realFileSize, size)
	}
}

func TestGetFileChunk(t *testing.T) {
	// Test we can get a chunk of a file
	fmt.Println("Test get file chunk...")
	cf := NewCloudFiles(TestUserName, TestApiKey)
	err := cf.Authorize()

	if err != nil {
		t.Fatalf("Could not authorize: %s", err)
	}

	size, _, err := cf.GetFileSize("IAD", "testing", "ubuntu-14.04.4-desktop-amd64.iso")
	if err != nil {
		t.Fatalf("Could not get file size: %s", err)
	}

	if size == 0 {
		t.Fatalf("Size should be greater than 0.")
	}

	tmpFile, err := ioutil.TempFile("", "")
	defer os.Remove(tmpFile.Name())

	reportedSize, _, err := cf.GetChunk("IAD", "testing", "ubuntu-14.04.4-desktop-amd64.iso",
		tmpFile, 100, 100000)

	tmpFile.Close()

	if err != nil {
		t.Fatalf("Could not get chunk: %s", err)
	}

	if reportedSize != 100000 {
		t.Fatalf("Bytes copied does not match: %d", reportedSize)
	}

	info, err := os.Stat(tmpFile.Name())
	if err != nil {
		t.Fatalf("Could not stat file: %s", err)
	}

	if info.Size() != 100000 {
		t.Fatalf("Expected file size to be 100,000 but got: %d", info.Size())
	}

}

func TestPutFileChunk(t *testing.T) {
	// Test we can put a file chunk
	fmt.Println("Test put file chunk...")
	cf := NewCloudFiles(TestUserName, TestApiKey)
	err := cf.Authorize()

	if err != nil {
		t.Fatalf("Could not authorize: %s", err)
	}

	buffer := make([]byte, 10000)
	_, err = rand.Read(buffer)
	if err != nil {
		t.Fatalf("Could not generate random: %s", err)
	}

	reader := bytes.NewReader(buffer)

	etag, err := cf.PutFile("IAD", "testing", "newfile.bin", reader)
	if err != nil {
		t.Fatalf("Could not put file: %s", err)
	}

	if etag == "" {
		t.Fatalf("Etag is empty but should be filled.")
	}

}

func TestCopyFile(t *testing.T) {
	// Test we can copy one file from DC to DC.
	fmt.Println("Test copy file...")
	cf := NewCloudFiles(TestUserName, TestApiKey)
	err := cf.Authorize()

	if err != nil {
		t.Fatalf("Could not authorize: %s", err)
	}

	err = cf.CopyFile("IAD", "testing", "ubuntu-14.04.4-desktop-amd64.iso",
		"DFW", "testing", "ubuntu-14.04.4-desktop-amd64.iso")

	if err != nil {
		t.Fatalf("Could not copy file: %s", err)
	}
}
