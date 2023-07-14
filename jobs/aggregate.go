package jobs

import (
	"fmt"
	"time"

	"github.com/application-research/edge-ur/core"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-merkledag"
	"github.com/multiformats/go-multihash"
)

type BucketAggregator struct {
	Bucket *core.Bucket
	Processor
}

func NewBucketAggregator(ln *core.LightNode, bucket *core.Bucket) IProcessor {
	return &BucketAggregator{
		Bucket: bucket,
		Processor: Processor{
			LightNode: ln,
		},
	}
}

func (r *BucketAggregator) Info() error {
	panic("implement me")
}

// Run is the main function of the BucketAggregator struct. It is responsible for aggregating the contents of a bucket
func (r *BucketAggregator) Run() error {
	// check if there are open bucket. if there are, generate the car file for the bucket.

	//var buckets []core.Bucket
	//r.LightNode.DB.Model(&core.Bucket{}).Where("status = ?", "open").Find(&buckets)

	// for each bucket, get all the contents and check if the total size is greater than the aggregate size limit (default 1GB)
	// if it is, generate a car file for the bucket and update the bucket status to processing
	//for _, bucket := range buckets {
	var content []core.Content
	query := "bucket_uuid = ?"
	r.LightNode.DB.Model(&core.Content{}).Where(query, r.Bucket.Uuid).Find(&content)

	// get total size of the bucket
	var totalSize int64
	r.LightNode.DB.Model(&core.Content{}).Where(query, r.Bucket.Uuid).Select("sum(size) as size").Scan(&totalSize)

	// get bucket policy
	var policy core.Policy
	r.LightNode.DB.Model(&core.Policy{}).Where("id = ?", r.Bucket.PolicyId).Find(&policy)

	// if policy is not set, create a default policy
	if policy.ID == 0 {
		// create a default policy
		newPolicy := core.Policy{
			Name:       r.Bucket.Name,
			BucketSize: r.LightNode.Config.Common.BucketAggregateSize,
			SplitSize:  r.LightNode.Config.Common.SplitSize,
			CreatedAt:  time.Now(),
			UpdatedAt:  time.Now(),
		}
		r.LightNode.DB.Save(&newPolicy)
		r.Bucket.PolicyId = newPolicy.ID
		policy = newPolicy
	}

	// check if total size is greater than the bucket size
	fmt.Println("Total size: ", totalSize, " Policy Bucket size: ", policy.BucketSize)
	if totalSize > policy.BucketSize {
		fmt.Println("Generating car file for bucket: ", r.Bucket.Uuid)
		r.Bucket.Status = "processing"
		r.LightNode.DB.Save(&r.Bucket)

		// process the car generator
		job := CreateNewDispatcher()
		genCar := NewBucketCarGenerator(r.LightNode, *r.Bucket)
		job.AddJob(genCar)
		job.Start(1)
	}

	return nil

}

// GetCidBuilderDefault is a helper function that returns a default cid builder
func GetCidBuilderDefault() cid.Builder {
	cidBuilder, err := merkledag.PrefixForCidVersion(1)
	if err != nil {
		panic(err)
	}
	cidBuilder.MhType = uint64(multihash.SHA2_256)
	cidBuilder.MhLength = -1
	return cidBuilder
}
