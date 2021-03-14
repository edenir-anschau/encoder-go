package services

import (
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type VideoUpload struct {
	Paths        []string
	VideoPath    string
	OutputBucket string
	Errors       []string
}

func NewVideoUpload() *VideoUpload {
	return &VideoUpload{}
}

func (vu *VideoUpload) UploadObject(objectPath string, uploader *s3manager.Uploader, worker int) error {
	path := strings.Split(objectPath, os.Getenv("localStoragePath")+"/")

	f, err := os.Open(objectPath)
	if err != nil {
		return err
	}
	defer f.Close()

	log.Printf("Uploading file %s with worker %d", path[1], worker)
	result, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(vu.OutputBucket),
		Key:    aws.String(path[1]),
		Body:   f,
	})
	if err != nil {
		log.Println("Couldn't upload file: ", err)
		return err
	}

	log.Printf("file %s uploaded to, %s\n", path[1], result.Location)

	return nil
}

func (vu *VideoUpload) ProcessUpload(concurrency int, doneUpload chan string) error {
	in := make(chan int, runtime.NumCPU())
	returnChannel := make(chan string)

	err := vu.loadPaths()
	if err != nil {
		return err
	}

	uploader := s3manager.NewUploader(getSession())
	for process := 0; process < concurrency; process++ {
		go vu.uploadWorker(in, returnChannel, uploader, process)
		log.Printf("Workder %d has started", process)
	}

	go func() {
		for x := 0; x < len(vu.Paths); x++ {
			in <- x
		}
		close(in)
	}()

	for r := range returnChannel {
		if r != "" {
			doneUpload <- r
			break
		}
	}

	return nil
}

func (vu *VideoUpload) uploadWorker(in chan int, returnChan chan string, uploader *s3manager.Uploader, worker int) {
	for x := range in {
		err := vu.UploadObject(vu.Paths[x], uploader, worker)
		if err != nil {
			vu.Errors = append(vu.Errors, vu.Paths[x])
			log.Printf("error during the upload: %v. Error: %v", vu.Paths[x], err)
			returnChan <- err.Error()
		}
	}
	returnChan <- "upload completed"
}

func (vu *VideoUpload) loadPaths() error {
	err := filepath.Walk(vu.VideoPath, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			vu.Paths = append(vu.Paths, path)
		}
		return nil
	})

	if err != nil {
		log.Println("Couldn't find paths at: ", vu.VideoPath)
		return err
	}
	log.Println("Path size: ", len(vu.Paths))
	return nil
}

func getSession() *session.Session {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1")},
	)
	if err != nil {
		log.Fatal("Error to create session", err)
	}
	return sess
}
