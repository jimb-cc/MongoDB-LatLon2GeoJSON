package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"

	"github.com/TylerBrock/colorjson"
)

func main() {
	fmt.Println("-- Lat/Lon conversion to GeoJSON")

	client, err := mongo.NewClient("mongodb://localhost")
	if err != nil {
		log.Fatal(err)
	}
	err = client.Connect(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	//db := client.Database("test")
	//connectDB(db)
	var str string
	str = `{
		"str": "foo",
		"num": 100,
		"bool": false,
		"null": null,
		"array": ["foo", "bar", "baz"],
		"obj": { "a": 1, "b": 2 }
	  }`

	testpretty(str)
}

func connectDB(db *mongo.Database) {
	coll := db.Collection("ais")

	cur, err := coll.Find(context.Background(), nil)
	if err != nil {
		log.Fatal(err)
	}
	defer cur.Close(context.Background())

	for cur.Next(context.Background()) {
		elem := bson.NewDocument()
		err := cur.Decode(elem)
		if err != nil {
			log.Fatal(err)
		}

		s, _ := colorjson.Marshal(elem)

		fmt.Println(string(s))
	}
	if err := cur.Err(); err != nil {
		log.Fatal(err)
	}

}

func testpretty(str string) {

	var obj map[string]interface{}
	json.Unmarshal([]byte(str), &obj)
	f := colorjson.NewFormatter()
	f.Indent = 2
	s, _ := f.Marshal(obj)
	fmt.Println(string(s))

}
