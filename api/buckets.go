package api

import (
	"github.com/application-research/edge-ur/core"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"strconv"
	"strings"
	"time"
)

type BucketsResponse struct {
	BucketUUID      string `json:"bucket_uuid"`
	Miner           string `json:"miner,omitempty"`
	PieceCommitment struct {
		PieceCid        string `json:"piece_cid"`
		PaddedPieceSize int64  `json:"padded_piece_size"`
	} `json:"piece_commitment"`
	PieceCid           string `json:"piece_cid"`
	PayloadCid         string `json:"payload_cid"`
	DirCid             string `json:"dir_cid"`
	TransferParameters struct {
		URL string `json:"url"`
	} `json:"transfer_parameters"`
	CollectionName string         `json:"collection_name"`
	Status         string         `json:"status"`
	Size           int64          `json:"size"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
	Contents       []core.Content `json:"contents"`
}

// ConfigureBucketsRouter The function ConfigureBucketsRouter configures the routing for various bucket-related endpoints in an Echo web
// framework.
func ConfigureBucketsRouter(e *echo.Group, node *core.LightNode) {
	//var DeltaUploadApi = node.Config.Delta.ApiUrl
	buckets := e.Group("/buckets")
	buckets.GET("/get/ready", handleGetOpenBuckets(node))
	buckets.GET("/get/open", handleGetOpenBuckets(node))
	//buckets.GET("/get/open", handleGetInProgressBuckets(node))
	buckets.GET("/get/in-progress", handleGetInProgressBuckets(node))
	buckets.GET("/get/processing", handleGetInProgressBuckets(node))
	buckets.POST("/create", handleCreateBucket(node))
	buckets.DELETE("/:uuid", handleDeleteBucket(node))
}

type CreateBucketRequest struct {
	Name       string `json:"name"`
	BucketSize int64  `json:"bucket_size"` // creates the policy
	SplitSize  int64  `json:"split_size"`  // if its more than 0
}

// The function `handleGetInProgressBuckets` retrieves a list of in-progress buckets and their associated contents.
func handleGetInProgressBuckets(node *core.LightNode) func(c echo.Context) error {
	return func(c echo.Context) error {
		pageNum, err := strconv.Atoi(c.QueryParam("page"))
		if err != nil || pageNum <= 0 {
			pageNum = 1
		}

		pageSize, err := strconv.Atoi(c.QueryParam("page_size"))
		if err != nil || pageSize <= 0 {
			pageSize = 25
		}

		var buckets []core.Bucket
		offset := (pageNum - 1) * pageSize
		node.DB.Model(&core.Bucket{}).Where("status = ?", "processing").Offset(offset).Limit(pageSize).Find(&buckets)

		var bucketsResponse []BucketsResponse
		for _, bucket := range buckets {
			response := BucketsResponse{
				BucketUUID:     bucket.Uuid,
				PieceCid:       bucket.PieceCid,
				PayloadCid:     bucket.Cid,
				DirCid:         bucket.DirCid,
				Status:         bucket.Status,
				CollectionName: bucket.Name,
				Size:           bucket.Size,
				CreatedAt:      bucket.CreatedAt,
				UpdatedAt:      bucket.UpdatedAt,
			}
			response.PieceCommitment.PaddedPieceSize = bucket.PieceSize
			response.PieceCommitment.PieceCid = bucket.PieceCid
			response.TransferParameters.URL = node.Api.Scheme + node.Config.Node.GwHost + "/gw/" + bucket.Cid
			bucketsResponse = append(bucketsResponse, response)

			// get all the content
			var contents []core.Content
			node.DB.Model(&core.Content{}).Where("bucket_uuid = ?", bucket.Uuid).Find(&contents)
			for i := range contents {
				contents[i].RequestingApiKey = ""
			}
			bucketsResponse[len(bucketsResponse)-1].Contents = contents
		}

		if len(bucketsResponse) == 0 {
			return c.JSON(404, map[string]interface{}{
				"message":     "No open buckets found.",
				"description": "This means that there are no buckets that are open and/processing.",
			})
		}
		return c.JSON(200, bucketsResponse)
	}
}

// The function `handleCreateBucket` handles the creation of a new bucket/tag in a database, with authorization and error
// handling.
func handleCreateBucket(node *core.LightNode) func(c echo.Context) error {
	return func(c echo.Context) error {

		// authorize
		authorizationString := c.Request().Header.Get("Authorization")
		authParts := strings.Split(authorizationString, " ")
		if authParts[1] != node.Config.Node.AdminApiKey {
			return c.JSON(401, map[string]interface{}{
				"message": "Unauthorized",
			})
		}

		// get the name
		var createBucketRequest CreateBucketRequest
		err := c.Bind(&createBucketRequest)
		if err != nil {
			return c.JSON(400, map[string]interface{}{
				"message": "Please provide a name for tag/bucket",
			})
		}
		tagName := createBucketRequest.Name
		if tagName == "" {
			return c.JSON(400, map[string]interface{}{
				"message": "Please provide a name for tag/bucket",
			})
		}

		// query if the tag name exist
		var bucket core.Bucket
		node.DB.Raw("select * from buckets as b where name = ?", tagName).Scan(&bucket)
		if bucket.ID != 0 {
			return c.JSON(400, map[string]interface{}{
				"message": "Collection name already exist",
			})
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
				Name:             tagName,
				RequestingApiKey: authParts[1],
				Uuid:             bucketUuid.String(),
				CreatedAt:        time.Now(),
				UpdatedAt:        time.Now(),
			}
			node.DB.Create(&bucket)
		}
		return nil
	}
}

// The function `handleDeleteBucket` handles the deletion of a bucket if called by the admin API key.
func handleDeleteBucket(node *core.LightNode) func(c echo.Context) error {
	return func(c echo.Context) error {

		// check if its being called by the admin api key
		authorizationString := c.Request().Header.Get("Authorization")
		authParts := strings.Split(authorizationString, " ")
		if authParts[1] != node.Config.Node.AdminApiKey {
			return c.JSON(401, map[string]interface{}{
				"message": "Unauthorized",
			})
		}

		node.DB.Model(&core.Bucket{}).Where("uuid = ?", c.Param("uuid")).Update("status", "deleted")
		return c.JSON(200, map[string]interface{}{
			"message": "Bucket deleted",
			"bucket":  c.Param("uuid"),
		})
	}
}

// The function `handleGetOpenBuckets` handles the GET request for retrieving a list of open buckets with pagination.
func handleGetOpenBuckets(node *core.LightNode) func(c echo.Context) error {
	return func(c echo.Context) error {
		assignMiner := c.QueryParam("miner")

		pageNum, err := strconv.Atoi(c.QueryParam("page"))
		if err != nil || pageNum <= 0 {
			pageNum = 1
		}

		pageSize, err := strconv.Atoi(c.QueryParam("page_size"))
		if err != nil || pageSize <= 0 {
			pageSize = 25
		}

		var buckets []core.Bucket
		offset := (pageNum - 1) * pageSize
		node.DB.Model(&core.Bucket{}).Where("status = ?", "ready").Offset(offset).Limit(pageSize).Find(&buckets)

		var bucketsResponse []BucketsResponse
		for _, bucket := range buckets {
			response := BucketsResponse{
				BucketUUID:     bucket.Uuid,
				PieceCid:       bucket.PieceCid,
				PayloadCid:     bucket.Cid,
				DirCid:         bucket.DirCid,
				Status:         bucket.Status,
				CollectionName: bucket.Name,
				Size:           bucket.Size,
				CreatedAt:      bucket.CreatedAt,
				UpdatedAt:      bucket.UpdatedAt,
				Miner: func() string {
					if assignMiner != "" {
						return assignMiner
					}
					return ""
				}(),
			}
			response.PieceCommitment.PaddedPieceSize = bucket.PieceSize
			response.PieceCommitment.PieceCid = bucket.PieceCid
			response.TransferParameters.URL = node.Api.Scheme + node.Config.Node.GwHost + "/gw/" + bucket.Cid
			bucketsResponse = append(bucketsResponse, response)

		}

		if len(bucketsResponse) == 0 {
			return c.JSON(404, map[string]interface{}{
				"message":     "No open buckets found.",
				"description": "This means that there are no buckets that are ready for deal making.",
			})
		}
		return c.JSON(200, bucketsResponse)
	}
}
