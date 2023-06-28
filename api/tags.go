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

func ConfigureTagsRouter(e *echo.Group, node *core.LightNode) {
	//var DeltaUploadApi = node.Config.Delta.ApiUrl
	buckets := e.Group("/tags")
	buckets.GET("/create", handleCreateTag(node))
	buckets.POST("/modify", handleModifyTag(node))
	buckets.DELETE("/remove/:tag-name", handleDeleteBucket(node))

}
func handleCreateTag(node *core.LightNode) func(c echo.Context) error {
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
				"message": "Tag name already exist",
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

func handleModifyTag(node *core.LightNode) func(c echo.Context) error {
	return func(c echo.Context) error {

		return nil
	}
}
