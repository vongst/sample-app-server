package main

import (
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	_ "github.com/heroku/x/hmetrics/onload"
)

func main() {
	port := os.Getenv("PORT")

	if port == "" {
		port = "8000"
		// log.Fatal("$PORT must be set")
	}

	router := gin.New()
	router.Use(gin.Logger())
	router.LoadHTMLGlob("templates/*.tmpl.html")
	router.Static("/static", "static")

	router.GET("/customers", func(c *gin.Context) {
		c.String(http.StatusOK, "{customers: 0}")
	})

	router.GET("/customers/:uid", func(c *gin.Context) {
		uid := c.Param("uid")
		c.String(http.StatusOK, "Hello"+uid)
	})

	router.Run(":" + port)
}
