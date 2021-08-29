package main

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

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

	jsonFilePath, _ := filepath.Abs("./data/out.json")

	type Customer struct {
		ID          int               `json:"id"`
		Attributes  map[string]string `json:"attributes"`
		Events      map[string]int    `json:"events"`
		LastUpdated int               `json:"last_updated"`
	}
	var data = []*Customer{}

	var jsonFile, err = ioutil.ReadFile(jsonFilePath)
	if err != nil {
		log.Fatal(err)
	}

	var stream string = string(jsonFile)
	var dd = json.NewDecoder(strings.NewReader(stream))

	for { // for each  new line
		var err = dd.Decode(&data)
		if err != nil {
			if err != io.EOF {
				log.Fatal(err)
			}
			break
		}
	}

	router.GET("/customers", func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")

		// sort by descending user id
		sort.SliceStable(data, func(i, j int) bool {
			return data[i].ID > data[j].ID
		})

		output, _ := json.Marshal(data)

		s := string(output)

		c.String(http.StatusOK, s)
	})

	router.GET("/customers/:uid", func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")

		str_uid := c.Param("uid")
		uid, err := strconv.Atoi(str_uid)

		if err == nil {
			for i := range data {
				if data[i].ID == uid {
					output, _ := json.Marshal(data[i])
					s := string(output)

					c.String(http.StatusOK, s)
				}
			}
		}

	})

	router.Run(":" + port)
}
