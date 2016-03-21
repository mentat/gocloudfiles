# Go Cloud Files

A parallelized golang library for Cloud Files.

## Usage

``` go
package "gocloudfiles"

// Create a new client
cf := gocloudfiles.NewCloudFiles(myUserName, myApiKey)
// Authorize this client.
err := cf.Authorize()
// Get a file or chunk of a file and write it to some io.Writer
length, etag, err := cf.GetChunk(myDc, myBucket, myFilename, outputWritier, 0, 0)
```

## Documentation

### NewCloudFiles(userName, apiKey string)

Create a new cloud files client using given username and apiKey.  Returns
a new CloudFiles client object.

### Cloudfiles.Authorize() error

Authorize user against the identity service in order to load the service
catalog into the object.

Returns: error

### GetFileSize(dc, bucket, filename string)

Get the size of a file in CloudFiles, returns the size, an etag, and any error.
This is done efficiently so the file data is not downloaded.

Returns: (size int64, etag string, err error)

### GetChunk(dc, bucket, remoteFilename string, out io.Writer, offset, length int64)

Get a chunk of a file starting at offset and reading length bytes.  If length
is zero the entire file will be downloaded.  Writes data to the given io.Writer.

Returns: (etag string, err error)

###  PutFile(dc, bucket, filename string, data io.Reader)

Put a file to Cloud Files using the given dc/bucket/filename.  Data is read from
the given io.Reader.

Returns: (etag string, err error)

### CopyFile(sourceDC, sourceBucket, sourceFile, destDC, destBucket, destFile string)

Copy a file from one source dc/bucket/filename to another.  This is done
using the static large file method and attempts to parallize the process.

Returns: error

## Testing

    export TEST_USERNAME="blah"
    export TEST_KEY="blah"

    go test
