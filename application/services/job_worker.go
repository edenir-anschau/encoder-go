package services

import (
	"encoder/domain"
	"encoder/framework/utils"
	"encoding/json"
	"log"
	"os"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/streadway/amqp"
)

type JobWorkerResult struct {
	Job     domain.Job
	Message *amqp.Delivery
	Error   error
}

func JobWorker(messageChannel chan amqp.Delivery, returnChan chan JobWorkerResult, jobService JobService, job domain.Job, workerID int) {
	log.Print("Starting workderID ", workerID)
	for message := range messageChannel {
		err := utils.IsJson(message.Body)
		if err != nil {
			log.Fatalf("Invalid JSON %v", err)
			returnChan <- returnJobResult(domain.Job{}, message, err)
			continue
		}

		err = json.Unmarshal(message.Body, &jobService.VideoService.Video)
		if err != nil {
			log.Printf("Error to unmarshal JSON %v", err)
			returnChan <- returnJobResult(domain.Job{}, message, err)
			continue
		}
		jobService.VideoService.Video.ID = uuid.NewV4().String()
		err = jobService.VideoService.Video.Validate()
		if err != nil {
			log.Printf("Invalid video %v", err)
			returnChan <- returnJobResult(domain.Job{}, message, err)
			continue
		}

		err = jobService.VideoService.Insert()
		if err != nil {
			returnChan <- returnJobResult(domain.Job{}, message, err)
			continue
		}

		job.Video = jobService.VideoService.Video
		job.OutputBucketPath = os.Getenv("outputBucketName")
		job.ID = uuid.NewV4().String()
		job.Status = "STARTING"
		job.CreatedAt = time.Now()

		_, err = jobService.JobRepository.Insert(&job)

		if err != nil {
			returnChan <- returnJobResult(domain.Job{}, message, err)
			continue
		}

		jobService.Job = &job
		err = jobService.Start()

		if err != nil {
			returnChan <- returnJobResult(domain.Job{}, message, err)
			continue
		}

		returnChan <- returnJobResult(job, message, nil)
	}
}

func returnJobResult(job domain.Job, message amqp.Delivery, err error) JobWorkerResult {
	result := JobWorkerResult{
		Job:     job,
		Message: &message,
		Error:   err,
	}
	return result
}
