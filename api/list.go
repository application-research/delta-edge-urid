package api

import (
	"strconv"
	"strings"

	"github.com/application-research/edge-ur/core"
	"github.com/labstack/echo/v4"
)

type ListResponse struct {
	Content struct {
		ID             int64  `json:"id"`
		Name           string `json:"name"`
		DeltaContentId int64  `json:"delta_content_id,omitempty"`
		Cid            string `json:"cid,omitempty"`
		Status         string `json:"status"`
		Message        string `json:"message,omitempty"`
	} `json:"content"`
}

func ConfigureListResponseRouter(e *echo.Group, node *core.LightNode) {
	e.GET("/list", func(c echo.Context) error {

		authorizationString := c.Request().Header.Get("Authorization")
		authParts := strings.Split(authorizationString, " ")
		pageStr := c.QueryParam("page")
		pageSizeStr := c.QueryParam("pageSize")
		// Parse page and pageSize parameters
		page, err := strconv.Atoi(pageStr)
		if err != nil || page < 1 {
			page = 1
		}

		pageSize, err := strconv.Atoi(pageSizeStr)
		if err != nil || pageSize < 1 {
			pageSize = 10
		}

		var contentCids []core.Content
		var totalContentCount int64

		// Get the total count of content records
		node.DB.Model(&core.Content{}).Where("requesting_api_key = ?", authParts[1]).Count(&totalContentCount)

		// Calculate the offset and limit for the current page
		offset := (page - 1) * pageSize
		limit := pageSize

		// Retrieve the content records for the current page
		node.DB.Where("requesting_api_key = ?", authParts[1]).Offset(offset).Limit(limit).Find(&contentCids)

		for _, content := range contentCids {
			content.RequestingApiKey = ""
		}

		if len(contentCids) == 0 {
			return c.JSON(404, map[string]interface{}{
				"message": "Content not found. Please check if you have the proper API key or if the content id is valid",
			})
		}

		return c.JSON(200, map[string]interface{}{
			"cids": contentCids,
		})
	})
}
