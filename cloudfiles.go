package gocloudfiles

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strconv"
)

type cloudFilesAuth struct {
	UserName string `json:"username"`
	ApiKey   string `json:"apiKey"`
}

type raxKeyCreds struct {
	Credentials cloudFilesAuth `json:"RAX-KSKEY:apiKeyCredentials"`
}

type serviceEndpoints struct {
	Region    string `json:"region"`
	TenantId  string `json:"tenantId"`
	PublicURL string `json:"publicURL"`
}

type serviceCatalog struct {
	Name      string             `json:"name"`
	Endpoints []serviceEndpoints `json:"endpoints"`
}

type tenantData struct {
	Id   string `json:"id"`
	Name string `json:"name"`
}

type tokenData struct {
	Id     string     `json:"id"`
	Tenant tenantData `json:"tenant"`
}

type serviceAccess struct {
	Catalog []serviceCatalog `json:"serviceCatalog"`
	Token   tokenData        `json:"token"`
}

type accessWrapper struct {
	Access serviceAccess `json:"access"`
}

type manifestItem struct {
	Path string `json:"path"`
	ETag string `json:"etag"`
	Size int64  `json:"size_bytes"`
}

// Create interface for sorting
type manifestList []manifestItem

func (slice manifestList) Len() int {
	return len(slice)
}

func (slice manifestList) Less(i, j int) bool {
	return slice[i].Path < slice[j].Path
}

func (slice manifestList) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

type CloudFiles struct {
	userName    string
	apiEndpoint string
	tenantId    string
	authToken   string
	apiKey      string
	dcs         map[string]string
}

func NewCloudFiles(userName, apiKey string) *CloudFiles {
	/*
	   Create a new cloud files object.
	*/
	cf := &CloudFiles{
		userName: userName,
		apiKey:   apiKey,
		dcs:      make(map[string]string),
	}

	return cf
}

func (cf *CloudFiles) Authorize() error {
	/*
	   Authorize against the identity service.
	*/
	client := &http.Client{}

	url := "https://identity.api.rackspacecloud.com/v2.0/tokens"

	authData := make(map[string]interface{})
	authData["auth"] = raxKeyCreds{
		Credentials: cloudFilesAuth{
			UserName: cf.userName,
			ApiKey:   cf.apiKey,
		},
	}

	payLoad, err := json.Marshal(authData)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(payLoad))

	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/json")
	resp, err := client.Do(req)

	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode != 200 {

		responseBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("Could not authenticate: %d", resp.StatusCode)
		} else {
			return fmt.Errorf("Could not authenticate: %s (%d)", responseBody, resp.StatusCode)
		}
	}

	var respData accessWrapper
	err = json.NewDecoder(resp.Body).Decode(&respData)

	if err != nil {
		return err
	}

	cf.authToken = respData.Access.Token.Id
	cf.tenantId = respData.Access.Token.Tenant.Id

	// Load all endpoints into memory.
	catalog := respData.Access.Catalog
	for i := range catalog {
		if catalog[i].Name == "cloudFiles" {
			endpoints := catalog[i].Endpoints
			for inner := range endpoints {
				cf.dcs[endpoints[inner].Region] = endpoints[inner].PublicURL
			}
			break
		}
	}

	return nil
}

func (cf CloudFiles) GetFileSize(dc, bucket, filename string) (int64, string, error) {
	/*
		Get the size of a remote cloudfiles file.
		Returns a 3-tuple of length, etag, error
	*/

	endpoint := cf.dcs[dc]
	if endpoint == "" {
		return 0, "", fmt.Errorf("Could not find region %s in service catalog.", dc)
	}

	client := &http.Client{}

	url := fmt.Sprintf("%s/%s/%s", endpoint, bucket, filename)

	req, err := http.NewRequest("HEAD", url, nil)
	//req.Header.Add("Range", "0")
	req.Header.Add("X-Auth-Token", cf.authToken)
	resp, err := client.Do(req)

	if resp.StatusCode != 200 {
		return 0, "", fmt.Errorf("Could not fetch cloud file, status: %d", resp.StatusCode)
	}

	contentLength, err := strconv.ParseInt(resp.Header["Content-Length"][0], 10, 64)

	if err != nil {
		return 0, "", fmt.Errorf("Could not determine content length.")
	}

	return contentLength, resp.Header["Etag"][0], nil
}

func (cf CloudFiles) GetChunk(dc, bucket, remoteFilename string, out io.Writer,
	offset, length int64) (size int64, etag string, err error) {
	/*
	   Write a cloud files chunk to the given io Writer.
	   out - must be closed by caller.
	*/

	endpoint := cf.dcs[dc]
	if endpoint == "" {
		return 0, "", fmt.Errorf("Could not find region %s in service catalog.", dc)
	}

	client := &http.Client{}

	url := fmt.Sprintf("%s/%s/%s", endpoint, bucket, remoteFilename)

	req, err := http.NewRequest("GET", url, nil)

	// The range includes the offset byte, so remove one from the end
	if length > 0 {
		req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	}
	req.Header.Add("X-Auth-Token", cf.authToken)

	// Get response...
	resp, err := client.Do(req)

	// Support response and partial response
	if resp.StatusCode != 200 && resp.StatusCode != 206 {
		return 0, "", fmt.Errorf("Could not fetch cloud file, status: %d", resp.StatusCode)
	}

	defer resp.Body.Close()

	// ETags...so the etag returned is always the etag of the entire file, so
	// we must generate a new one if the chunk represented only a portion of
	// the file...
	if length > 0 {
		// But obivously we'd rather not keep the entire file in memory and
		// would prefer to stream it.  We do this via a multi-writer.
		hasher := md5.New()
		multi := io.MultiWriter(hasher, out)
		size, err = io.Copy(multi, resp.Body)

		if err == nil {
			etag = hex.EncodeToString(hasher.Sum([]byte{}))
		}

	} else {
		size, err = io.Copy(out, resp.Body)
		etag = resp.Header["Etag"][0]
	}

	if err != nil {
		return 0, "", err
	}

	return size, etag, nil
}

func (cf CloudFiles) PutFile(dc, bucket, filename string, data io.Reader) (string, error) {
	/*
	   Write the data in io.Reader to Cloudfiles.
	   Returns a tuple of etag, error
	*/
	endpoint := cf.dcs[dc]
	if endpoint == "" {
		return "", fmt.Errorf("Could not find region %s in service catalog.", dc)
	}

	client := &http.Client{}

	url := fmt.Sprintf("%s/%s/%s", endpoint, bucket, filename)

	req, err := http.NewRequest("PUT", url, data)

	req.Header.Add("Content-Type", "application/octet-stream")
	req.Header.Add("X-Auth-Token", cf.authToken)
	resp, err := client.Do(req)

	// Support response and partial response
	if resp.StatusCode != 201 {
		return "", fmt.Errorf("Could not put cloud file, status: %d", resp.StatusCode)
	}

	if err != nil {
		return "", err
	}

	return resp.Header["Etag"][0], nil
}

func (cf CloudFiles) putManifest(dc, bucket, filename string, manifestItems manifestList) error {
	endpoint := cf.dcs[dc]

	if endpoint == "" {
		return fmt.Errorf("Could not find region %s in service catalog.", dc)
	}

	// Sort manifest
	sort.Sort(manifestItems)

	payLoad, err := json.Marshal(manifestItems)
	if err != nil {
		return err
	}

	client := &http.Client{}

	url := fmt.Sprintf("%s/%s/%s?multipart-manifest=put", endpoint, bucket, filename)

	req, err := http.NewRequest("PUT", url, bytes.NewReader(payLoad))
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("X-Auth-Token", cf.authToken)
	resp, err := client.Do(req)

	// Support response and partial response
	if resp.StatusCode != 201 {
		defer resp.Body.Close()
		errorMessage := new(bytes.Buffer)
		errorMessage.ReadFrom(resp.Body)

		return fmt.Errorf("Could not put cloud file manifest, status: %d, error: %s",
			resp.StatusCode, errorMessage.String())
	}

	if err != nil {
		return err
	}

	return nil
}

func (cf CloudFiles) CopyFile(sourceDC, sourceBucket, sourceFile, destDC, destBucket, destFile string) error {
	/*
		Copy a file from source cloudfiles to dest cloudfiles.
	*/
	// 256MB chunks, tune as needed
	chunkSize := int64(256 * 1024 * 1024)

	size, _, err := cf.GetFileSize(sourceDC, sourceBucket, sourceFile)
	if err != nil {
		return err
	}

	chunkCount := size / chunkSize
	remainder := size % chunkSize

	if remainder > 0 {
		chunkCount++
	}

	// Create a place to store all of our manifest items
	manifests := make(manifestList, 0, chunkCount)

	// Create semaphore for concurrency
	concurrency := 5
	sem := make(chan bool, concurrency)

	// Create other communication channels
	errorChan := make(chan error, concurrency)
	manifestChan := make(chan manifestItem, concurrency)

	var processError error = nil

	// Loop through all chunks and create goroutines for each...
	// The number of active goroutines is limited by the length of sem
loop:
	for chunkId := int64(0); chunkId < chunkCount; chunkId++ {
		sem <- true

		go func(chunkIndex int64, ec chan error, mf chan manifestItem) {
			defer func() { <-sem }()

			tmpFile, err := ioutil.TempFile("", "")
			defer os.Remove(tmpFile.Name())

			if err != nil {
				//  This would be bad...
				ec <- err
				return
			}

			size := chunkSize

			if chunkIndex == (chunkCount - 1) {
				size = remainder
			}

			// Download the file.
			bytesRead, etag, err := cf.GetChunk(sourceDC, sourceBucket, sourceFile,
				tmpFile, chunkIndex*chunkSize, size)

			if err != nil {
				ec <- err
				tmpFile.Close()
				return
			}

			tmpFile.Sync()
			tmpFile.Seek(0, 0)

			// The destination file name of the "part".
			destFileName := fmt.Sprintf("%s-%d", destFile, chunkIndex)

			// Smart recovery, first check the etag of the chunk/file to put
			// and determine if we should actually upload.
			_, etagUp, err := cf.GetFileSize(destDC, destBucket, destFileName)

			if err == nil && etagUp == etag {
				// File already exists in remote DC, don't upload again.
			} else {
				etagUp, err = cf.PutFile(destDC, destBucket,
					destFileName, tmpFile)

				if err != nil {
					ec <- err
					return
				}
			}

			if etagUp != etag {
				ec <- fmt.Errorf("Upload etag does not match download etag: %s %s!", etag, etagUp)
				return
			}

			manifest := manifestItem{
				Path: fmt.Sprintf("%s/%s", destBucket, destFileName),
				ETag: etag,
				Size: bytesRead,
			}

			mf <- manifest

			// get the url
		}(chunkId, errorChan, manifestChan)

		select {
		case err := <-errorChan:
			// Handle download/upload errors
			fmt.Printf("Oh no, error: %s\n", err)
			processError = err
			break loop
		case manifest := <-manifestChan:
			manifests = append(manifests, manifest)
		default:
			// Do nothing allow semaphore to continue loading jobs
		}
	}

	// Fill the semaphone channel back up to ensure
	// all operations have completed.
	for i := 0; i < cap(sem); i++ {
		sem <- true

		// Again read data coming from channels
		select {
		case err := <-errorChan:
			// Handle download/upload errors
			fmt.Printf("Oh no, error: %s", err)
			processError = err
			break
		case manifest := <-manifestChan:
			manifests = append(manifests, manifest)
		default:
			// Do nothing allow semaphore to continue clearing jobs
		}
	}

	// Handle any errors passed from the goroutines
	if processError != nil {
		return processError
	}

	err = cf.putManifest(destDC, destBucket, destFile, manifests)

	if err != nil {
		return err
	}

	return nil
}
