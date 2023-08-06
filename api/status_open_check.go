package api

import (
	"context"
	"github.com/application-research/edge-ur/core"
	"github.com/application-research/edge-ur/jobs"
	"github.com/ipfs/go-cid"
	"github.com/labstack/echo/v4"
)

type StatusOpenCheckResponse struct {
	Content struct {
		ID             int64  `json:"id"`
		Name           string `json:"name"`
		DeltaContentId int64  `json:"delta_content_id,omitempty"`
		Cid            string `json:"cid,omitempty"`
		Status         string `json:"status"`
		Message        string `json:"message,omitempty"`
	} `json:"content"`
}

// ConfigureStatusOpenCheckRouter The function `ConfigureStatusOpenCheckRouter` sets up routes for checking the status of content, buckets, and tags in an
// Echo web server.
func ConfigureStatusOpenCheckRouter(e *echo.Group, node *core.LightNode) {
	e.GET("/status/cid/:cid", func(c echo.Context) error {

		var contentCids []core.Content
		node.DB.Raw("select * from contents as c where cid = ?", c.Param("cid")).Scan(&contentCids)

		for _, content := range contentCids {
			content.RequestingApiKey = ""
		}

		if len(contentCids) == 0 {
			return c.JSON(404, map[string]interface{}{
				"message": "Content not found. Please check if the content id is valid",
			})
		}

		return c.JSON(200, map[string]interface{}{
			"cids": contentCids,
		})
	})
	e.GET("/status/content/:contentId", func(c echo.Context) error {

		var content core.Content
		node.DB.Raw("select * from contents as c where id = ?", c.Param("contentId")).Scan(&content)
		content.RequestingApiKey = ""

		if content.ID == 0 {
			return c.JSON(404, map[string]interface{}{
				"message": "Content not found. Please check if the content id is valid",
			})
		}
		return c.JSON(200, map[string]interface{}{
			"content": content,
		})
	})
	e.GET("/status/bucket/:bucketUuid", func(c echo.Context) error {

		var bucket core.Bucket
		node.DB.Model(&core.Bucket{}).Where("uuid = ?", c.Param("bucketUuid")).Scan(&bucket)

		// get the cid
		bucketCid, err := cid.Decode(bucket.Cid)
		if err != nil {
			return c.JSON(404, map[string]interface{}{
				"message": "Bucket not found. Please check if the bucket id is valid",
			})
		}
		dirNdRaw, err := node.Node.DAGService.Get(context.Background(), bucketCid)
		if err != nil {
			return c.JSON(404, map[string]interface{}{
				"message": "Bucket not found. Please check if the bucket id is valid",
			})
		}

		var contents []core.Content
		node.DB.Model(&core.Content{}).Where("bucket_uuid = ?", c.Param("uuid")).Scan(&contents)

		var contentResponse []core.Content
		for _, content := range contents {
			content.RequestingApiKey = ""

			for _, link := range dirNdRaw.Links() {
				cidFromDb, err := cid.Decode(content.Cid)
				if err != nil {
					continue
				}
				if link.Cid == cidFromDb {
					content.Cid = link.Cid.String()
					content.RequestingApiKey = ""
					contentResponse = append(contentResponse, content)
				}

			}
		}

		bucket.RequestingApiKey = ""
		return c.JSON(200, map[string]interface{}{
			"bucket":        bucket,
			"content_links": contentResponse,
		})
	})
	e.GET("/status/tag/:tag-name", func(c echo.Context) error {

		var bucket core.Bucket
		node.DB.Model(&core.Bucket{}).Where("collection_name = ?", c.Param("tag-name")).Scan(&bucket)

		// get the cid
		bucketCid, err := cid.Decode(bucket.Cid)
		if err != nil {
			return c.JSON(404, map[string]interface{}{
				"message": "Bucket not found. Please check if the bucket id is valid",
			})
		}
		dirNdRaw, err := node.Node.DAGService.Get(context.Background(), bucketCid)
		if err != nil {
			return c.JSON(404, map[string]interface{}{
				"message": "Bucket not found. Please check if the bucket id is valid",
			})
		}

		var contents []core.Content
		node.DB.Model(&core.Content{}).Where("bucket_uuid = ?", c.Param("uuid")).Scan(&contents)

		var contentResponse []core.Content
		for _, content := range contents {
			content.RequestingApiKey = ""

			for _, link := range dirNdRaw.Links() {
				cidFromDb, err := cid.Decode(content.Cid)
				if err != nil {
					continue
				}
				if link.Cid == cidFromDb {
					content.Cid = link.Cid.String()
					content.RequestingApiKey = ""
					contentResponse = append(contentResponse, content)
				}

			}

		}
		// trigger status check
		job := jobs.CreateNewDispatcher()
		job.Start(len(contents) + 1)

		bucket.RequestingApiKey = ""
		return c.JSON(200, map[string]interface{}{
			"bucket":        bucket,
			"content_links": contentResponse,
		})
	})
}
