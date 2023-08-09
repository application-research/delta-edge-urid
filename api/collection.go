package api

import (
	"github.com/application-research/edge-ur/core"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"strings"
	"time"
)

type CreateTagRequest struct {
	Name       string `json:"name"`
	BucketSize int64  `json:"bucket_size"` // creates the policy
	SplitSize  int64  `json:"split_size"`  // if its more than 0
}

func ConfigureCollectionsRouter(e *echo.Group, node *core.LightNode) {
	//var DeltaUploadApi = node.Config.Delta.ApiUrl
	collections := e.Group("/collections")
	collections.GET("/get", handleGetCollections(node))
	collections.GET("/create", handleCreateCollection(node))
	collections.POST("/modify", handleModifyCollection(node))
	//buckets.DELETE("/remove/:collection-name", handleDeleteBucket(node))

}

// The function `handleGetCollections` handles the GET request for retrieving collections based on a provided name.
func handleGetCollections(node *core.LightNode) func(c echo.Context) error {
	return func(c echo.Context) error {

		tagName := c.QueryParam("name")

		if tagName == "" {
			return c.JSON(400, map[string]interface{}{
				"message": "Please provide a collection name",
			})
		}

		var buckets []core.Bucket
		node.DB.Model(&core.Bucket{}).Where("status = ? and name = ?", "ready", tagName).Find(&buckets)

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

// The function `handleCreateCollection` handles the creation of a new collection (tag/bucket) by checking authorization,
// validating the name, and creating a new bucket if the name does not already exist.
func handleCreateCollection(node *core.LightNode) func(c echo.Context) error {
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
		var createTagRequest CreateTagRequest
		err := c.Bind(&createTagRequest)
		if err != nil {
			return c.JSON(400, map[string]interface{}{
				"message": "Please provide a name for tag/bucket",
			})
		}
		tagName := createTagRequest.Name
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

// The function handleModifyCollection returns a function that handles modifying a collection.
func handleModifyCollection(node *core.LightNode) func(c echo.Context) error {
	return func(c echo.Context) error {
		return nil
	}
}
