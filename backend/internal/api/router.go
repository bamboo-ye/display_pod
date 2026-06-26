package api

import (
	"net/http"
	"strconv"

	"display-pod/backend/internal/config"
	"display-pod/backend/internal/repository"
	"display-pod/backend/internal/ws"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

func NewRouter(cfg config.Config, papers *repository.PaperRepository, hub *ws.Hub) *gin.Engine {
	router := gin.Default()
	router.Use(cors.New(cors.Config{
		AllowOrigins:     []string{cfg.FrontendOrigin},
		AllowMethods:     []string{"GET", "POST", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type"},
		AllowCredentials: true,
	}))

	api := router.Group("/api")
	api.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	api.GET("/papers", func(c *gin.Context) {
		limit := queryInt(c, "limit", 20)
		offset := queryInt(c, "offset", 0)
		year := queryInt(c, "year", 0)
		items, err := papers.List(c.Request.Context(), c.Query("q"), year, limit, offset)
		respond(c, items, err)
	})
	api.GET("/stats/yearly", func(c *gin.Context) {
		data, err := papers.YearlyStats(c.Request.Context())
		respond(c, data, err)
	})
	api.GET("/stats/summary", func(c *gin.Context) {
		data, err := papers.Summary(c.Request.Context())
		respond(c, data, err)
	})
	api.GET("/stats/years", func(c *gin.Context) {
		data, err := papers.Years(c.Request.Context())
		respond(c, data, err)
	})
	api.GET("/stats/keywords", func(c *gin.Context) {
		data, err := papers.KeywordStats(c.Request.Context(), queryInt(c, "limit", 80))
		respond(c, data, err)
	})
	api.GET("/stats/authors", func(c *gin.Context) {
		data, err := papers.AuthorStats(c.Request.Context(), queryInt(c, "limit", 20))
		respond(c, data, err)
	})
	api.GET("/stats/institutions", func(c *gin.Context) {
		data, err := papers.InstitutionStats(c.Request.Context(), queryInt(c, "limit", 20))
		respond(c, data, err)
	})
	api.GET("/ws", func(c *gin.Context) {
		ws.Serve(hub, cfg.FrontendOrigin, c.Writer, c.Request)
	})
	return router
}

func respond(c *gin.Context, data any, err error) {
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": data})
}

func queryInt(c *gin.Context, key string, fallback int) int {
	value, err := strconv.Atoi(c.Query(key))
	if err != nil {
		return fallback
	}
	return value
}
