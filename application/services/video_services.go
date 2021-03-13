package services

import (
	"encoder/domain"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
)

var localStoragePath string = os.Getenv("localStoragePath")

type VideoService struct {
	Video *domain.Video
}

func NewVideoService() VideoService {
	return VideoService{}
}

func (v *VideoService) Download(bucketName string) error {
	content, err := ioutil.ReadFile(os.Getenv("localStoragePath") + "/" + bucketName + "/" + v.Video.FilePath)
	if err != nil {
		return err
	}

	f, err := os.Create(localStoragePath + "/" + v.Video.ID + ".mp4")
	if err != nil {
		return err
	}

	_, err = f.Write(content)
	if err != nil {
		return err
	}
	defer f.Close()

	log.Printf("video %v has been stored at", v.Video.ID)
	return nil
}

func (v *VideoService) Fragment() error {
	err := os.Mkdir(localStoragePath+"/"+v.Video.ID, os.ModePerm)
	if err != nil {
		return err
	}

	source := localStoragePath + "/" + v.Video.ID + ".mp4"
	target := localStoragePath + "/" + v.Video.ID + ".frag"

	cmd := exec.Command("mp4fragment", source, target)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return err
	}
	printOutput(output)
	return nil
}

func (v *VideoService) Encode() error {
	cmdArgs := []string{}
	cmdArgs = append(cmdArgs, localStoragePath+"/"+v.Video.ID+".frag")
	cmdArgs = append(cmdArgs, "--use-segment-timeline")
	cmdArgs = append(cmdArgs, "-o")
	cmdArgs = append(cmdArgs, localStoragePath+"/"+v.Video.ID)
	cmdArgs = append(cmdArgs, "-f")
	cmdArgs = append(cmdArgs, "--exec-dir")
	cmdArgs = append(cmdArgs, "/opt/bento4/bin/")
	cmd := exec.Command("mp4dash", cmdArgs...)

	output, err := cmd.CombinedOutput()

	if err != nil {
		return err
	}

	printOutput(output)

	return nil
}

func (v *VideoService) Finish() error {
	err := os.Remove(localStoragePath + "/" + v.Video.ID + ".mp4")
	if err != nil {
		log.Println("error removing mp4", v.Video.ID, ".mp4")
		return err
	}
	log.Println("file has been removed", v.Video.ID, ".mp4")

	err = os.Remove(localStoragePath + "/" + v.Video.ID + ".frag")
	if err != nil {
		log.Println("error removing frag", v.Video.ID, ".frag")
		return err
	}
	log.Println("file has been removed", v.Video.ID, ".frag")

	err = os.RemoveAll(localStoragePath + "/" + v.Video.ID)
	if err != nil {
		return err
	}
	log.Println("files has been removed: ", v.Video.ID)
	return nil
}

func printOutput(out []byte) {
	if len(out) > 0 {
		log.Printf("======> Output: %s\n", string(out))
	}
}
