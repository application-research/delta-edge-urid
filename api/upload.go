package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/application-research/edge-ur/jobs"
	"github.com/application-research/edge-ur/utils"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car"
	"github.com/labstack/echo/v4"
	"io"
	"strings"
	"time"

	"github.com/application-research/edge-ur/core"
)

type CidRequest struct {
	Cids []string `json:"cids"`
}

type DealE2EUploadRequest struct {
	Cid                string `json:"cid,omitempty"`
	Miner              string `json:"miner,omitempty"`
	Duration           int64  `json:"duration,omitempty"`
	DurationInDays     int64  `json:"duration_in_days,omitempty"`
	ConnectionMode     string `json:"connection_mode,omitempty"`
	Size               int64  `json:"size,omitempty"`
	StartEpoch         int64  `json:"start_epoch,omitempty"`
	StartEpochInDays   int64  `json:"start_epoch_in_days,omitempty"`
	RemoveUnsealedCopy bool   `json:"remove_unsealed_copy"`
	SkipIPNIAnnounce   bool   `json:"skip_ipni_announce"`
	AutoRetry          bool   `json:"auto_retry"`
	Label              string `json:"label,omitempty"`
	DealVerifyState    string `json:"deal_verify_state,omitempty"`
}

// DealE2EUploadResponse DealResponse Creating a new struct called DealResponse and then returning it.
type DealE2EUploadResponse struct {
	Status                       string      `json:"status"`
	Message                      string      `json:"message"`
	ContentId                    int64       `json:"content_id,omitempty"`
	DealRequest                  interface{} `json:"deal_request_meta,omitempty"`
	DealProposalParameterRequest interface{} `json:"deal_proposal_parameter_request_meta,omitempty"`
	ReplicatedContents           interface{} `json:"replicated_contents,omitempty"`
}

type UploadSplitResponse struct {
	Status        string              `json:"status"`
	Message       string              `json:"message"`
	RootCid       string              `json:"rootCid,omitempty"`
	RootContentId int64               `json:"rootContentId,omitempty"`
	Splits        []core.UploadSplits `json:"splits,omitempty"`
}

type PinResponse struct {
	Status     string `json:"status"`
	Message    string `json:"message"`
	ID         int64  `json:"id,omitempty"`
	Cid        string `json:"cid,omitempty"`
	ContentUrl string `json:"content_url,omitempty"`
}

type UploadResponse struct {
	Status       string      `json:"status"`
	Message      string      `json:"message"`
	ID           int64       `json:"id,omitempty"`
	Cid          string      `json:"cid,omitempty"`
	DeltaContent interface{} `json:"delta_content,omitempty"`
	ContentUrl   string      `json:"content_url,omitempty"`
}

// The function `ConfigureUploadRouter` configures the upload routes for a given Echo group and LightNode.
func ConfigureUploadRouter(e *echo.Group, node *core.LightNode) {
	content := e.Group("/content")
	content.POST("/add", handleUploadToCarBucket(node))
	content.POST("/upload", handleUploadToCarBucket(node))
	content.POST("/pin", handlePin(node))
	content.POST("/fetch-cids", handleFetchCids(node))
	content.POST("/car-cids", handleCidsToCarBucket(node))
	content.POST("/add-car", handleUploadCarToBucket(node))
}

type SignedUrlRequest struct {
	Message             string    `json:"message"`
	CurrentTimestamp    time.Time `json:"current_timestamp"`
	ExpirationTimestamp time.Time `json:"expiration_timestamp"`
	EdgeContentId       int64     `json:"edge_content_id"`
	Signature           string    `json:"signature"`
}

// The function `handlePin` handles the pinning of a file to IPFS and returns a JSON response with the status, CID, content
// URL, and message.
func handlePin(node *core.LightNode) func(c echo.Context) error {
	return func(c echo.Context) error {
		file, err := c.FormFile("data")
		if err != nil {
			return err
		}
		src, err := file.Open()
		if err != nil {
			return err
		}

		addNode, err := node.Node.AddPinFile(c.Request().Context(), src, nil)
		if err != nil {
			return c.JSON(500, UploadResponse{
				Status:  "error",
				Message: "Error adding the file to IPFS",
			})
		}
		cid := addNode.Cid().String()
		return c.JSON(200, UploadResponse{
			Status:     "success",
			Cid:        cid,
			ContentUrl: node.Config.Node.GwHost + "/gw/" + cid,
			Message:    "File pinned successfully",
		})
	}
}

// The function `handleFetchCids` handles a request to fetch and pin multiple CIDs, returning a JSON response with the
// status and message for each CID.
func handleFetchCids(node *core.LightNode) func(c echo.Context) error {
	return func(c echo.Context) error {
		cidBodyReq := CidRequest{}
		c.Bind(&cidBodyReq)

		type PinCidsResponse struct {
			Cid     string `json:"cid"`
			Status  string `json:"status"`
			Message string `json:"message"`
		}
		var pinCidsResponse []PinCidsResponse
		for _, cidItem := range cidBodyReq.Cids {
			cidDc, err := cid.Decode(cidItem)
			pinCidsResponseItem := PinCidsResponse{
				Cid: cidItem,
			}
			if err != nil {
				pinCidsResponseItem.Status = "error"
				pinCidsResponseItem.Message = "Error decoding the CID"
				pinCidsResponse = append(pinCidsResponse, pinCidsResponseItem)
				continue
			}

			_, errGet := node.Node.Get(c.Request().Context(), cidDc)
			if errGet != nil {
				pinCidsResponseItem.Status = "error"
				pinCidsResponseItem.Message = "Error fetching the CID"
				pinCidsResponse = append(pinCidsResponse, pinCidsResponseItem)
				continue
			}
			pinCidsResponseItem.Status = "success"
			pinCidsResponseItem.Message = "CID successfully fetched and pinned"
		}
		return c.JSON(200, pinCidsResponse)
	}
}

// The function `handleCidsToCarBucket` handles the request to upload and pin files to a bucket in IPFS, splitting the
// files if necessary and applying tag policies.
func handleCidsToCarBucket(node *core.LightNode) func(c echo.Context) error {
	return func(c echo.Context) error {
		authorizationString := c.Request().Header.Get("Authorization")
		authParts := strings.Split(authorizationString, " ")
		cidBodyReq := CidRequest{}
		c.Bind(&cidBodyReq)
		collectionName := c.FormValue("collection_name")

		// Check capacity if needed
		if node.Config.Common.CapacityLimitPerKeyInBytes > 0 {
			if err := validateCapacityLimit(node, authParts[1]); err != nil {
				return c.JSON(500, UploadResponse{
					Status:  "error",
					Message: err.Error(),
				})
			}
		}

		// check if tag exists, if it does, get the ID
		fmt.Println(collectionName)
		if collectionName == "" {
			collectionName = node.Config.Node.DefaultCollectionName
		}

		// load the policy of the tag
		var policy core.Policy
		node.DB.Where("name = ?", collectionName).First(&policy)
		if policy.ID == 0 {
			// create new policy
			newPolicy := core.Policy{
				Name:       collectionName,
				BucketSize: node.Config.Common.BucketAggregateSize,
				SplitSize:  node.Config.Common.SplitSize,
				CreatedAt:  time.Now(),
				UpdatedAt:  time.Now(),
			}
			node.DB.Create(&newPolicy)
			policy = newPolicy
		}

		// check open bucket
		var contentList []core.Content
		job := jobs.CreateNewDispatcher()
		for _, cidItem := range cidBodyReq.Cids {
			time.Sleep(5 * time.Second)
			cidDc, err := cid.Decode(cidItem)
			if err != nil {
				return c.JSON(500, UploadResponse{
					Status:  "error",
					Message: "Error decoding the CID",
				})
			}

			nodeToGet, err := node.Node.GetFile(c.Request().Context(), cidDc)
			if err != nil {
				return c.JSON(500, UploadResponse{
					Status:  "error",
					Message: "Error adding the file to IPFS",
				})
			}
			var nodeW bytes.Buffer
			_, errS := io.Copy(&nodeW, nodeToGet)
			if errS != nil {
				return c.JSON(500, UploadResponse{
					Status:  "error",
					Message: "Error getting the file size",
				})
			}

			nodeFileSize := nodeW.Len()

			if int64(nodeFileSize) > node.Config.Common.MaxSizeToSplit {
				newContent := core.Content{
					Name:             cidDc.String(),
					Size:             int64(nodeFileSize),
					Cid:              cidDc.String(),
					CollectionName:   collectionName,
					RequestingApiKey: authParts[1],
					Status:           utils.STATUS_PINNED,
					MakeDeal:         true,
					CreatedAt:        time.Now(),
					UpdatedAt:        time.Now(),
				}

				node.DB.Create(&newContent)

				// split the file and use the same tag policies
				job.AddJob(jobs.NewSplitterProcessor(node, newContent, nodeToGet))
				job.Start(1)
				if err != nil {
					return c.JSON(500, UploadResponse{
						Status:  "error",
						Message: "Error pinning the file" + err.Error(),
					})
				}
				newContent.RequestingApiKey = ""
				contentList = append(contentList, newContent)
			} else {
				var bucket core.Bucket

				rawQuery := "SELECT * FROM buckets WHERE status = ? and name = ?"
				node.DB.Raw(rawQuery, "open", collectionName).First(&bucket)

				if bucket.ID == 0 {
					// create a new bucket
					bucketUuid, errUuid := uuid.NewUUID()
					if errUuid != nil {
						return c.JSON(500, UploadResponse{
							Status:  "error",
							Message: "Error creating bucket",
						})
					}
					bucket = core.Bucket{
						Status:           "open",
						Name:             collectionName,
						RequestingApiKey: authParts[1],
						Uuid:             bucketUuid.String(),
						PolicyId:         policy.ID,
						Size:             int64(nodeFileSize),
						CreatedAt:        time.Now(),
						UpdatedAt:        time.Now(),
					}
					node.DB.Create(&bucket)
				} else {

					bucket.Size = bucket.Size + int64(nodeFileSize)
					node.DB.Save(&bucket)
				}
				fmt.Println("bucketUuid", bucket.Uuid, "bucket.Size", bucket.Size)

				newContent := core.Content{
					Name:             cidDc.String(),
					Size:             int64(nodeFileSize),
					Cid:              cidDc.String(),
					RequestingApiKey: authParts[1],
					Status:           utils.STATUS_PINNED,
					CollectionName:   collectionName,
					BucketUuid:       bucket.Uuid,
					MakeDeal:         true,
					CreatedAt:        time.Now(),
					UpdatedAt:        time.Now(),
				}

				node.DB.Create(&newContent)

				// bucket aggregator

				job.AddJob(jobs.NewBucketAggregator(node, &bucket))
				job.Start(1)
				if err != nil {
					return c.JSON(500, UploadResponse{
						Status:  "error",
						Message: "Error pinning the file" + err.Error(),
					})
				}
				newContent.RequestingApiKey = ""
				contentList = append(contentList, newContent)
			}
			//}
		}

		//job.Start(len(cidBodyReq.Cids))

		return c.JSON(200, struct {
			Status   string         `json:"status"`
			Message  string         `json:"message"`
			Contents []core.Content `json:"contents"`
		}{
			Status:   "success",
			Message:  "File uploaded and pinned successfully. Please take note of the ids.",
			Contents: contentList,
		})
	}
}

// The function `handleUploadToCarBucket` handles the upload of a file to a bucket in a car storage system, including
// splitting the file if necessary and updating the bucket's size.
func handleUploadToCarBucket(node *core.LightNode) func(c echo.Context) error {
	return func(c echo.Context) error {
		authorizationString := c.Request().Header.Get("Authorization")
		authParts := strings.Split(authorizationString, " ")
		collectionName := c.FormValue("collection_name")

		// Check capacity if needed
		if node.Config.Common.CapacityLimitPerKeyInBytes > 0 {
			if err := validateCapacityLimit(node, authParts[1]); err != nil {
				return c.JSON(500, UploadResponse{
					Status:  "error",
					Message: err.Error(),
				})
			}
		}

		// check if tag exists, if it does, get the ID
		fmt.Println(collectionName)
		if collectionName == "" {
			collectionName = node.Config.Node.DefaultCollectionName
		}

		// load the policy of the tag
		var policy core.Policy
		node.DB.Where("name = ?", collectionName).First(&policy)
		if policy.ID == 0 {
			// create new policy
			newPolicy := core.Policy{
				Name:       collectionName,
				BucketSize: node.Config.Common.BucketAggregateSize,
				SplitSize:  node.Config.Common.SplitSize,
				CreatedAt:  time.Now(),
				UpdatedAt:  time.Now(),
			}
			node.DB.Create(&newPolicy)
			policy = newPolicy
		}

		file, err := c.FormFile("data")
		if err != nil {
			return err
		}
		src, err := file.Open()
		srcR, err := file.Open()
		if err != nil {
			return err
		}

		addNode, err := node.Node.AddPinFile(c.Request().Context(), src, nil)
		if err != nil {
			return c.JSON(500, UploadResponse{
				Status:  "error",
				Message: "Error adding the file to IPFS",
			})
		}

		// check open bucket
		var contentList []core.Content

		if file.Size > node.Config.Common.MaxSizeToSplit {
			newContent := core.Content{
				Name:             file.Filename,
				Size:             file.Size,
				Cid:              addNode.Cid().String(),
				CollectionName:   collectionName,
				RequestingApiKey: authParts[1],
				Status:           utils.STATUS_PINNED,
				MakeDeal:         true,
				CreatedAt:        time.Now(),
				UpdatedAt:        time.Now(),
			}

			node.DB.Create(&newContent)

			// split the file and use the same tag policies
			job := jobs.CreateNewDispatcher()
			job.AddJob(jobs.NewSplitterProcessor(node, newContent, srcR))
			job.Start(1)

			if err != nil {
				return c.JSON(500, UploadResponse{
					Status:  "error",
					Message: "Error pinning the file" + err.Error(),
				})
			}
			newContent.RequestingApiKey = ""
			contentList = append(contentList, newContent)
		} else {
			var bucket core.Bucket

			rawQuery := "SELECT * FROM buckets WHERE status = ? and name = ?"
			node.DB.Raw(rawQuery, "open", collectionName).First(&bucket)

			if bucket.ID == 0 {
				// create a new bucket
				bucketUuid, errUuid := uuid.NewUUID()
				if errUuid != nil {
					return c.JSON(500, UploadResponse{
						Status:  "error",
						Message: "Error creating bucket",
					})
				}
				bucket = core.Bucket{
					Status:           "open",
					Name:             collectionName,
					RequestingApiKey: authParts[1],
					Uuid:             bucketUuid.String(),
					PolicyId:         policy.ID,
					Size:             file.Size,
					CreatedAt:        time.Now(),
					UpdatedAt:        time.Now(),
				}
				node.DB.Create(&bucket)
			} else {

				bucket.Size = bucket.Size + file.Size
				node.DB.Save(&bucket)
			}
			fmt.Println("bucketUuid", bucket.Uuid, "bucket.Size", bucket.Size)

			newContent := core.Content{
				Name:             file.Filename,
				Size:             file.Size,
				Cid:              addNode.Cid().String(),
				RequestingApiKey: authParts[1],
				Status:           utils.STATUS_PINNED,
				CollectionName:   collectionName,
				BucketUuid:       bucket.Uuid,
				MakeDeal:         true,
				CreatedAt:        time.Now(),
				UpdatedAt:        time.Now(),
			}

			node.DB.Create(&newContent)

			// bucket aggregator
			job := jobs.CreateNewDispatcher()
			job.AddJob(jobs.NewBucketAggregator(node, &bucket))
			job.Start(1)

			if err != nil {
				return c.JSON(500, UploadResponse{
					Status:  "error",
					Message: "Error pinning the file" + err.Error(),
				})
			}
			newContent.RequestingApiKey = ""
			contentList = append(contentList, newContent)
		}
		//}

		return c.JSON(200, struct {
			Status   string         `json:"status"`
			Message  string         `json:"message"`
			Contents []core.Content `json:"contents"`
		}{
			Status:   "success",
			Message:  "File uploaded and pinned successfully. Please take note of the ids.",
			Contents: contentList,
		})
	}
}

// The function `handleUploadCarToBucket` handles the upload of a car file to a bucket, splitting the file if necessary and
// pinning it to the IPFS network.
func handleUploadCarToBucket(node *core.LightNode) func(c echo.Context) error {
	return func(c echo.Context) error {
		authorizationString := c.Request().Header.Get("Authorization")
		authParts := strings.Split(authorizationString, " ")
		collectionName := c.FormValue("collection_name")

		// Check capacity if needed
		if node.Config.Common.CapacityLimitPerKeyInBytes > 0 {
			if err := validateCapacityLimit(node, authParts[1]); err != nil {
				return c.JSON(500, UploadResponse{
					Status:  "error",
					Message: err.Error(),
				})
			}
		}

		// check if tag exists, if it does, get the ID
		fmt.Println(collectionName)
		if collectionName == "" {
			collectionName = node.Config.Node.DefaultCollectionName
		}

		// load the policy of the tag
		var policy core.Policy
		node.DB.Where("name = ?", collectionName).First(&policy)

		if policy.ID == 0 {
			newPolicy := core.Policy{
				Name:       collectionName,
				BucketSize: node.Config.Common.BucketAggregateSize,
				SplitSize:  node.Config.Common.SplitSize,
				CreatedAt:  time.Now(),
				UpdatedAt:  time.Now(),
			}
			node.DB.Create(&newPolicy)
			policy = newPolicy
		}

		file, err := c.FormFile("data")
		if err != nil {
			return err
		}
		src, err := file.Open()
		srcR, err := file.Open()
		if err != nil {
			return err
		}

		carHeader, err := car.LoadCar(context.Background(), node.Node.Blockstore, src)

		if err != nil {
			return c.JSON(500, UploadResponse{
				Status:  "error",
				Message: "Error loading car file: " + err.Error(),
			})
		}
		rootCid := carHeader.Roots[0].String()

		for _, root := range carHeader.Roots {
			fmt.Println("root.String()", root.String())
			rootCid = root.String()
		}

		// check open bucket
		var contentList []core.Content

		if file.Size > node.Config.Common.MaxSizeToSplit {
			newContent := core.Content{
				Name:             file.Filename,
				Size:             file.Size,
				Cid:              rootCid,
				CollectionName:   collectionName,
				RequestingApiKey: authParts[1],
				Status:           utils.STATUS_PINNED,
				MakeDeal:         true,
				CreatedAt:        time.Now(),
				UpdatedAt:        time.Now(),
			}

			node.DB.Create(&newContent)

			// split the file and use the same tag policies
			job := jobs.CreateNewDispatcher()
			job.AddJob(jobs.NewSplitterProcessor(node, newContent, srcR))
			job.Start(1)

			if err != nil {
				return c.JSON(500, UploadResponse{
					Status:  "error",
					Message: "Error pinning the file" + err.Error(),
				})
			}
			newContent.RequestingApiKey = ""
			contentList = append(contentList, newContent)
		} else {
			var bucket core.Bucket

			rawQuery := "SELECT * FROM buckets WHERE status = ? and name = ?"
			node.DB.Raw(rawQuery, "open", collectionName).First(&bucket)

			if bucket.ID == 0 {
				// create a new bucket
				bucketUuid, errUuid := uuid.NewUUID()
				if errUuid != nil {
					return c.JSON(500, UploadResponse{
						Status:  "error",
						Message: "Error creating bucket",
					})
				}
				bucket = core.Bucket{
					Status:           "open",
					Name:             collectionName,
					RequestingApiKey: authParts[1],
					//DeltaNodeUrl:     DeltaUploadApi,
					Uuid:     bucketUuid.String(),
					PolicyId: policy.ID,
					Size:     file.Size,
					//Miner:            miner, // blank
					//Tag:       tag,
					CreatedAt: time.Now(),
					UpdatedAt: time.Now(),
				}
				node.DB.Create(&bucket)
			} else {

				bucket.Size = bucket.Size + file.Size
				node.DB.Save(&bucket)
			}
			fmt.Println("bucketUuid", bucket.Uuid, "bucket.Size", bucket.Size)
			newContent := core.Content{
				Name: file.Filename,
				Size: file.Size,
				Cid:  rootCid,
				//DeltaNodeUrl:     DeltaUploadApi,
				RequestingApiKey: authParts[1],
				Status:           utils.STATUS_PINNED,
				//Miner:            miner,
				//Tag:        tag,
				CollectionName: collectionName,
				BucketUuid:     bucket.Uuid,
				MakeDeal:       true,
				CreatedAt:      time.Now(),
				UpdatedAt:      time.Now(),
			}

			node.DB.Create(&newContent)

			//if makeDeal == "true" {
			job := jobs.CreateNewDispatcher()
			job.AddJob(jobs.NewBucketAggregator(node, &bucket))
			job.Start(1)
			//}

			if err != nil {
				return c.JSON(500, UploadResponse{
					Status:  "error",
					Message: "Error pinning the file" + err.Error(),
				})
			}
			newContent.RequestingApiKey = ""
			contentList = append(contentList, newContent)
		}
		//}

		return c.JSON(200, struct {
			Status   string         `json:"status"`
			Message  string         `json:"message"`
			Contents []core.Content `json:"contents"`
		}{
			Status:   "success",
			Message:  "File uploaded and pinned successfully. Please take note of the ids.",
			Contents: contentList,
		})

	}
}

// The function `validateCapacityLimit` checks if the total size of contents associated with a given API key exceeds a
// specified capacity limit.
func validateCapacityLimit(node *core.LightNode, authKey string) error {
	var totalSize int64
	err := node.DB.Raw(`SELECT COALESCE(SUM(size), 0) FROM contents where requesting_api_key = ?`, authKey).Scan(&totalSize).Error
	if err != nil {
		return err
	}

	if totalSize >= node.Config.Common.CapacityLimitPerKeyInBytes {
		return errors.New(fmt.Sprintf("You have reached your capacity limit of %d bytes", node.Config.Common.CapacityLimitPerKeyInBytes))
	}

	return nil
}

type roots struct {
	Event    string `json:"event"`
	Payload  int    `json:"payload"`
	Stream   int    `json:"stream"`
	Cid      string `json:"cid"`
	Wiresize uint64 `json:"wiresize"`
}

func getRoots(rerr io.Reader) []roots {

	var rs []roots
	bs, _ := io.ReadAll(rerr)
	e := string(bs)
	els := strings.Split(e, "\n")
	for _, el := range els {
		if el == "" {
			continue
		}
		var r roots
		err := json.Unmarshal([]byte(el), &r)
		if err != nil {
			fmt.Printf("failed to unmarshal json: %s\n", el)
		}
		rs = append(rs, r)
	}
	return rs
}
