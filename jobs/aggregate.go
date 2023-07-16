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

func NewBucketAggregator(ln *core.LightNode) IProcessor {
	return &BucketAggregator{
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

	for {

		// get all open bucket
		var buckets []core.Bucket
		// bucket is open if status is open and size is more than policy size
		r.LightNode.DB.Model(&core.Bucket{}).Where("status = ? AND size > ?", "open", r.LightNode.Config.Common.BucketAggregateSize).Find(&buckets)

		// if there are no open buckets, break the loop
		//if len(buckets) == 0 {
		//	break
		//}

		// iterate through all the buckets
		for _, bucket := range buckets {
			// get total size of the bucket
			var totalSize int64
			r.LightNode.DB.Model(&core.Content{}).Where("bucket_uuid = ?", bucket.Uuid).Select("sum(size) as size").Scan(&totalSize)

			// get bucket policy
			var policy core.Policy
			r.LightNode.DB.Model(&core.Policy{}).Where("id = ?", bucket.PolicyId).Find(&policy)

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
				bucket.PolicyId = newPolicy.ID
				policy = newPolicy
			}

			// check if total size is greater than the bucket size
			fmt.Println("Total size: ", totalSize, " Policy Bucket size: ", policy.BucketSize)
			if totalSize > policy.BucketSize {
				fmt.Println("Generating car file for bucket: ", bucket.Uuid)
				bucket.Status = "processing"
				r.LightNode.DB.Save(&bucket)

				// process the car generator
				job := CreateNewDispatcher()
				genCar := NewBucketCarGenerator(r.LightNode, bucket)
				job.AddJob(genCar)
				job.Start(1)
			}
		}
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
