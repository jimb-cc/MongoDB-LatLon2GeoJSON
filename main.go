package main

// resources:
// - https://www.compose.com/articles/mongodb-and-go-moving-on-from-mgo/
// - https://gitlab.com/wemgl/todocli/blob/master/main.go
// - https://godoc.org/github.com/mongodb/mongo-go-driver/bson

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"
)

type aisRecord struct {
	//objectID       string `json:"id"`
	LRIMOShipNo    string
	ShipName       string
	ShipType       string
	MMSI           int32
	CallSign       string
	Latitude       float64
	Longitude      float64
	Length         int32
	Draught        float64
	Beam           int32
	Heading        float64
	Speed          float64
	Destination    string
	ETA            time.Time `json:"ETA"`
	MoveStatus     string
	MoveDateTime   time.Time `json:"MovementDateTime"`
	AdditionalInfo string
	MovementID     int64
}

func main() {
	fmt.Println("-- Lat/Lon conversion to GeoJSON")
	ctx := context.Background()

	client, err := mongo.NewClient("mongodb://localhost")
	if err != nil {
		log.Fatal(err)
	}

	err = client.Connect(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	db := client.Database("test")

	readDocs(ctx, db)
}

func readDocs(ctx context.Context, db *mongo.Database) error {
	c, err := db.Collection("ais").Find(ctx, nil)
	if err != nil {
		return fmt.Errorf("Couldn't find any data: %v", err)
	}
	defer c.Close(ctx)

	for c.Next(ctx) {
		elem := bson.NewDocument()
		if err = c.Decode(elem); err != nil {
			return fmt.Errorf("can't decode a doc: %v", err)
		}
		s := aisRecord{
			//objectID:       elem.Lookup("_id").StringValue(),
			LRIMOShipNo:    elem.Lookup("LRIMOShipNo").StringValue(),
			ShipName:       elem.Lookup("ShipName").StringValue(),
			ShipType:       elem.Lookup("ShipType").StringValue(),
			MMSI:           elem.Lookup("MMSI").Int32(),
			CallSign:       elem.Lookup("CallSign").StringValue(),
			Latitude:       elem.Lookup("Latitude").Double(),
			Longitude:      elem.Lookup("Longitude").Double(),
			Length:         elem.Lookup("Length").Int32(),
			Draught:        elem.Lookup("Draught").Double(),
			Beam:           elem.Lookup("Beam").Int32(),
			Heading:        elem.Lookup("Heading").Double(),
			Speed:          elem.Lookup("Speed").Double(),
			Destination:    elem.Lookup("Destination").StringValue(),
			ETA:            elem.Lookup("ETA").DateTime(),
			MoveStatus:     elem.Lookup("MoveStatus").StringValue(),
			MoveDateTime:   elem.Lookup("MovementDateTime").DateTime(),
			AdditionalInfo: elem.Lookup("AdditionalInfo").StringValue(),
			MovementID:     elem.Lookup("MovementID").Int64()}

		spew.Printf("%#v", s)

	}

	if err = c.Err(); err != nil {
		return fmt.Errorf("all data couldn't be listed: %v", err)
	}
	return nil
}
