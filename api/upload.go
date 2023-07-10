package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/application-research/edge-ur/jobs"
	"github.com/application-research/edge-ur/utils"
	"github.com/google/uuid"
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

type UploadResponse struct {
	Status       string      `json:"status"`
	Message      string      `json:"message"`
	ID           int64       `json:"id,omitempty"`
	Cid          string      `json:"cid,omitempty"`
	DeltaContent interface{} `json:"delta_content,omitempty"`
	ContentUrl   string      `json:"content_url,omitempty"`
}

func ConfigureUploadRouter(e *echo.Group, node *core.LightNode) {
	var DeltaUploadApi = node.Config.ExternalApi.DeltaSvcUrl
	content := e.Group("/content")
	//content.POST("/add", handleUpload(node, DeltaUploadApi))
	//content.POST("/add-car", handleUpload(node, DeltaUploadApi))

	// impossible cloud specific
	content.PUT("/put", handleUpload(node, DeltaUploadApi))
	//content.GET("/get", handleUploadCarToBucket(node, DeltaUploadApi))
	//content.GET("/track", handleUploadCarToBucket(node, DeltaUploadApi))
	content.GET("/list", handleUploadCarToBucket(node, DeltaUploadApi))

}

type SignedUrlRequest struct {
	Message             string    `json:"message"`
	CurrentTimestamp    time.Time `json:"current_timestamp"`
	ExpirationTimestamp time.Time `json:"expiration_timestamp"`
	EdgeContentId       int64     `json:"edge_content_id"`
	Signature           string    `json:"signature"`
}

func handleUpload(node *core.LightNode, DeltaUploadApi string) func(c echo.Context) error {
	return func(c echo.Context) error {
		authorizationString := c.Request().Header.Get("Authorization")
		authParts := strings.Split(authorizationString, " ")
		collectionName := c.FormValue("collection_name")
		objectId := c.FormValue("object_id")

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
		if collectionName == "" {
			collectionName = node.Config.Node.DefaultCollectionName
		}

		// load the policy of the tag
		var policy core.Policy
		node.DB.Where("name = ?", collectionName).First(&policy)

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

		//for miner := range miners {
		if file.Size > node.Config.Common.MaxSizeToSplit {
			newContent := core.Content{
				Name:             file.Filename,
				Size:             file.Size,
				Cid:              addNode.Cid().String(),
				CollectionName:   collectionName,
				RequestingApiKey: authParts[1],
				Status:           utils.STATUS_PINNED,
				CreatedAt:        time.Now(),
				UpdatedAt:        time.Now(),
			}

			node.DB.Create(&newContent)

			// split the file and use the same tag policies
			job := jobs.CreateNewDispatcher()
			job.AddJob(jobs.NewSplitterProcessor(node, newContent, srcR))
			job.Start(1)

			if err != nil {
				c.JSON(500, UploadResponse{
					Status:  "error",
					Message: "Error pinning the file" + err.Error(),
				})
			}
			newContent.RequestingApiKey = ""
			contentList = append(contentList, newContent)
		} else {
			var bucket core.Bucket

			if node.Config.Common.AggregatePerApiKey {
				rawQuery := "SELECT * FROM buckets WHERE status = ? and name = ? and requesting_api_key = ?"
				node.DB.Raw(rawQuery, "open", collectionName, authParts[1]).First(&bucket)
			} else {
				rawQuery := "SELECT * FROM buckets WHERE status = ? and name = ?"
				node.DB.Raw(rawQuery, "open", collectionName).First(&bucket)
			}

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
			}

			// check if object id exist
			var contentToModify core.Content
			node.DB.Where("object_id = ?", objectId).First(&contentToModify)
			if contentToModify.ID != 0 {

				contentToModify.Name = file.Filename
				contentToModify.Size = file.Size
				contentToModify.Cid = addNode.Cid().String()
				contentToModify.RequestingApiKey = authParts[1]
				contentToModify.Status = utils.STATUS_PINNED
				contentToModify.CollectionName = collectionName
				contentToModify.BucketUuid = bucket.Uuid
				contentToModify.UpdatedAt = time.Now()
				node.DB.Create(&contentToModify)

				job := jobs.CreateNewDispatcher()
				job.AddJob(jobs.NewBucketAggregator(node, contentToModify, srcR, false))
				job.Start(1)
				contentToModify.RequestingApiKey = ""
				contentList = append(contentList, contentToModify)

			} else {
				// new object id
				objectUuid, errObjId := uuid.NewUUID()
				if errObjId != nil {
					return c.JSON(500, UploadResponse{
						Status:  "error",
						Message: "Error creating object",
					})
				}

				newContent := core.Content{
					Name:             file.Filename,
					Size:             file.Size,
					Cid:              addNode.Cid().String(),
					RequestingApiKey: authParts[1],
					ObjectId:         objectUuid.String(),
					Status:           utils.STATUS_PINNED,
					CollectionName:   collectionName,
					BucketUuid:       bucket.Uuid,
					CreatedAt:        time.Now(),
					UpdatedAt:        time.Now(),
				}

				node.DB.Create(&newContent)
				job := jobs.CreateNewDispatcher()
				job.AddJob(jobs.NewBucketAggregator(node, newContent, srcR, false))
				job.Start(1)
				newContent.RequestingApiKey = ""
				contentList = append(contentList, newContent)
			}

		}
		//}

		c.JSON(200, struct {
			Status   string         `json:"status"`
			Message  string         `json:"message"`
			Contents []core.Content `json:"contents"`
		}{
			Status:   "success",
			Message:  "File uploaded and pinned successfully. Please take note of the ids.",
			Contents: contentList,
		})

		return nil
	}
}
func handleUploadCarToBucket(node *core.LightNode, DeltaUploadApi string) func(c echo.Context) error {
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
			c.JSON(500, UploadResponse{
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

		//for miner := range miners {
		fmt.Println("file.Size", file.Size)
		fmt.Println("node.Config.Common.MaxSizeToSplit", node.Config.Common.MaxSizeToSplit)

		if file.Size > node.Config.Common.MaxSizeToSplit {
			newContent := core.Content{
				Name:             file.Filename,
				Size:             file.Size,
				Cid:              rootCid,
				CollectionName:   collectionName,
				RequestingApiKey: authParts[1],
				Status:           utils.STATUS_PINNED,
				CreatedAt:        time.Now(),
				UpdatedAt:        time.Now(),
			}

			node.DB.Create(&newContent)

			// split the file and use the same tag policies
			job := jobs.CreateNewDispatcher()
			job.AddJob(jobs.NewSplitterProcessor(node, newContent, srcR))
			job.Start(1)

			if err != nil {
				c.JSON(500, UploadResponse{
					Status:  "error",
					Message: "Error pinning the file" + err.Error(),
				})
			}
			newContent.RequestingApiKey = ""
			contentList = append(contentList, newContent)
		} else {
			var bucket core.Bucket

			if node.Config.Common.AggregatePerApiKey {
				rawQuery := "SELECT * FROM buckets WHERE status = ? and name = ? and requesting_api_key = ?"
				node.DB.Raw(rawQuery, "open", collectionName, authParts[1]).First(&bucket)
			} else {
				rawQuery := "SELECT * FROM buckets WHERE status = ? and name = ?"
				node.DB.Raw(rawQuery, "open", collectionName).First(&bucket)
			}

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
			}

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
				CreatedAt:      time.Now(),
				UpdatedAt:      time.Now(),
			}

			node.DB.Create(&newContent)

			//if makeDeal == "true" {
			job := jobs.CreateNewDispatcher()
			job.AddJob(jobs.NewBucketAggregator(node, newContent, srcR, false))
			job.Start(1)
			//}

			if err != nil {
				c.JSON(500, UploadResponse{
					Status:  "error",
					Message: "Error pinning the file" + err.Error(),
				})
			}
			newContent.RequestingApiKey = ""
			contentList = append(contentList, newContent)
		}
		//}

		c.JSON(200, struct {
			Status   string         `json:"status"`
			Message  string         `json:"message"`
			Contents []core.Content `json:"contents"`
		}{
			Status:   "success",
			Message:  "File uploaded and pinned successfully. Please take note of the ids.",
			Contents: contentList,
		})

		return nil
	}
}
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
