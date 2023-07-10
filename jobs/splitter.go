package jobs

import (
	"bytes"
	"context"
	"github.com/application-research/edge-ur/core"
	"github.com/application-research/edge-ur/utils"
	"github.com/google/uuid"
	"io"
	"time"
)

type SplitterProcessor struct {
	Content core.Content `json:"content"`
	File    io.Reader    `json:"file"`
	Processor
}

func NewSplitterProcessor(ln *core.LightNode, contentToProcess core.Content, fileNode io.Reader) IProcessor {
	return &SplitterProcessor{
		contentToProcess,
		fileNode,
		Processor{
			LightNode: ln,
		},
	}
}

func (r *SplitterProcessor) Info() error {
	panic("implement me")
}

func (r *SplitterProcessor) Run() error {

	// load the policy
	var policy core.Policy
	r.LightNode.DB.Model(&core.Policy{}).Where("name = ?", r.Content.CollectionName).First(&policy)

	// split the file.
	fileSplitter := new(core.FileSplitter)
	fileSplitter.ChuckSize = policy.SplitSize
	arrBts, err := fileSplitter.SplitFileFromReader(r.File) // nice split.
	if err != nil {
		panic(err)
	}

	// create a bucket
	bucketUuid, err := uuid.NewUUID()
	bucket := core.Bucket{
		Status:           "open",
		Name:             r.Content.CollectionName,
		RequestingApiKey: r.Content.RequestingApiKey,
		Uuid:             bucketUuid.String(),
		Miner:            r.Content.Miner,
		PolicyId:         policy.ID,
		CreatedAt:        time.Now(),
		UpdatedAt:        time.Now(),
	}
	r.LightNode.DB.Create(&bucket)

	// create a content for each split
	var i int
	for _, b := range arrBts {
		bNd, err := r.LightNode.Node.AddPinFile(context.Background(), bytes.NewReader(b), nil)
		if err != nil {
			panic(err)
		}
		newContent := core.Content{
			Name: string(i) + "split-" + bNd.Cid().String(),
			Size: int64(len(b)),
			Cid:  bNd.Cid().String(),
			//DeltaNodeUrl:     r.Content.DeltaNodeUrl,
			RequestingApiKey: r.Content.RequestingApiKey,
			Status:           utils.STATUS_PINNED,
			Miner:            r.Content.Miner,
			CollectionName:   r.Content.CollectionName,
			BucketUuid:       bucket.Uuid,
			CreatedAt:        time.Now(),
			UpdatedAt:        time.Now(),
		}
		r.LightNode.DB.Create(&newContent)
		i++
	}

	job := CreateNewDispatcher()
	genCar := NewBucketCarGenerator(r.LightNode, bucket)
	job.AddJob(genCar)
	job.Start(1)

	return nil
}
