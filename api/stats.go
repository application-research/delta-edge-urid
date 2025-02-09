package api

import (
	"github.com/application-research/edge-ur/core"
	"github.com/labstack/echo/v4"
	"github.com/patrickmn/go-cache"
	"time"
)

var cacheStats = cache.New(48*time.Hour, 24*time.Hour)

type Stats struct {
	TotalContentCount int `json:"total_content_count"`
	TotalSize         int `json:"total_size"`
	TotalApiKeys      int `json:"total_api_keys"`
}

func ConfigureStatsRouter(e *echo.Group, node *core.LightNode) {
	e.GET("/stats", func(c echo.Context) error {

		stats, found := cacheStats.Get("stats")
		if found {
			return c.JSON(200, stats)
		}

		var s Stats
		err := node.DB.Raw("select * from mv_total_content_count").Scan(&s.TotalContentCount).Error
		if err != nil {
			return c.JSON(500, err)
		}

		err = node.DB.Raw("select * from mv_total_size").Scan(&s.TotalSize).Error
		if err != nil {
			return c.JSON(500, err)
		}

		//select sum(total_api_keys) from (select count(*) as total_api_keys from contents group by requesting_api_key) as total_api_keys;
		err = node.DB.Raw("select * from mv_total_api_keys").Scan(&s.TotalApiKeys).Error
		if err != nil {
			return c.JSON(500, err)
		}

		cacheStats.Set("stats", &s, cache.DefaultExpiration)
		return c.JSON(200, s)
	})
}
