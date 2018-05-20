package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"strconv"

	"github.com/olivere/elastic"
	"github.com/olivere/elastic/config"
	"github.com/qedus/osmpbf"
)

type Way struct {
	Nodes Nodes `json:"nodes"`
}

type Nodes struct {
	Type        string       `json:"type"`
	Coordinates [][2]float64 `json:"coordinates"`
}

type mapping struct {
	Mappings Mappings `json:"mappings"`
}

type Mappings struct {
	MappingWay MappingWay `json:"way"`
}

type MappingWay struct {
	MappingWayProperties MappingWayProperties `json:"properties"`
}

type MappingWayProperties struct {
	MappingWayPropertiesNodes MappingWayPropertiesNodes `json:"nodes"`
}

type MappingWayPropertiesNodes struct {
	Type string `json:"type"`
}

func createWayIndex() error {
	mapping := mapping{
		Mappings: Mappings{
			MappingWay: MappingWay{
				MappingWayProperties: MappingWayProperties{
					MappingWayPropertiesNodes: MappingWayPropertiesNodes{
						Type: "geo_shape",
					},
				},
			},
		},
	}

	// `
	// {
	//   "mappings": {
	//     "way": {
	//       "properties": {
	//         "nodes": {
	//           "type": "geo_shape"
	//         }
	//       }
	//     }
	//   }
	// }
	// `

	jsonBytes, err := json.Marshal(mapping)
	if err != nil {
		return err
	}

	fmt.Println(string(jsonBytes))

	_, err = client.CreateIndex("grabroad_map").BodyJson(mapping).Do(context.Background())
	if err != nil {
		return err
	}

	return nil
}

var (
	client *elastic.Client
)

const (
	url = "http://localhost:9200"
)

func inita() {
	cfg := &config.Config{
		URL: url,
	}
	c, err := elastic.NewClientFromConfig(cfg)
	if err != nil {
		panic(err)
	}

	client = c

	fmt.Println("inited", client)

	version, err := client.ElasticsearchVersion(url)
	if err != nil {
		panic(err)
	}

	fmt.Println("init elasticsearch version:", version)
}

func indexWays(reader io.Reader) error {
	d := osmpbf.NewDecoder(reader)

	// use more memory from the start, it is faster
	d.SetBufferSize(osmpbf.MaxBlobSize)

	// start decoding with several goroutines, it is faster
	err := d.Start(runtime.GOMAXPROCS(-1))
	if err != nil {
		return err
	}

	var (
		nodesMap = map[int64][2]float64{}
	)

	var rc uint64
	for {
		if v, err := d.Decode(); err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		} else {
			switch v := v.(type) {
			case *osmpbf.Node:
				// Process Node v.
				nodesMap[v.ID] = [2]float64{v.Lon, v.Lat}
			case *osmpbf.Way:
				// Process Way v.

				nodes := Nodes{Type: "linestring", Coordinates: [][2]float64{}}
				for _, nodeID := range v.NodeIDs {
					nodes.Coordinates = append(nodes.Coordinates, nodesMap[nodeID])
				}

				way := Way{Nodes: nodes}

				jsonBytes, err := json.Marshal(way)
				if err != nil {
					fmt.Println("aaa")
					return err
				}

				jsonStr := string(jsonBytes)
				fmt.Println("=====", jsonStr)

				indexResult, err := client.
					Index().
					Id(strconv.FormatInt(v.ID, 10)).
					Index("grabroad_map").
					Type("way").
					BodyString(jsonStr).
					Do(context.Background())

				fmt.Println("=================")
				if err != nil {
					return err
				}

				fmt.Println(indexResult)

			case *osmpbf.Relation:
				// Process Relation v.
				rc++
			default:
				return fmt.Errorf("unknown type %T\n", v)
			}
		}
	}

	return nil
}

type query struct {
}

func main() {
	inita()
	// mapFile, fileErr := os.Open("./map/sin.osm.pbf")
	// if fileErr != nil {
	// 	fmt.Println("map file err:", fileErr)
	// 	return
	// }

	// err := createWayIndex()
	// if err != nil {
	// 	fmt.Println("create index err:", err)
	// 	return
	// }

	// indexWaysErr := indexWays(mapFile)
	// if indexWaysErr != nil {
	// 	fmt.Println("index ways err:", indexWaysErr)
	// 	return
	// }

	queryStr := `
	{
        "geo_shape": {
          "nodes": {
            "shape": {
              "type":   "envelope",
              "coordinates": [
                [
                  103.9693678,
                  1.4025251
                ],
                [
                  104.9693678,
                  1.5025251
                ]
              ]
            }
          }
        }
      }

    `

	query := elastic.NewRawStringQuery(queryStr)

	result, err := client.Search().
		Index("grabroad_map").
		Type("way").
		Query(query).
		Do(context.Background())
	if err != nil {
		fmt.Println("query err:", err)
		return
	}

	// fmt.Printf("result: %#v", result)
	fmt.Println("result:", result.TotalHits())

	http.ListenAndServe("0.0.0.0:8080", nil)
}
