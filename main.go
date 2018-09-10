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

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"
)

// define connection URI
const uri = "mongodb://localhost"

// define database
const database = "AIS"

// define source and destination collections
const source = "ais_1m"
const dest = "ais_1m_fix"

func main() {
	fmt.Println("-- Lat/Lon conversion to GeoJSON")

	// create a context. I have no idea what this means. at all
	ctx := context.Background()

	// create a connection to the DB
	client, err := mongo.NewClient(uri)
	if err != nil {
		log.Fatal(err)
	}

	// this bit makes no sense at all. I may have just copy/paste it from somewhere
	err = client.Connect(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	// select the database to use
	db := client.Database(database)

	// drop the destination collection (naughty! must deal with errors!)
	_ = db.Collection(dest).Drop(ctx, nil)

	// note the start time
	start := time.Now()
	fmt.Printf("\n--Started at: %v\n", start)

	// start doing work
	readDocs(ctx, db)

	// note the end time
	finished := time.Now()
	fmt.Printf("\n--Finished at: %v\n", finished)
	fmt.Printf("--took: %v\n", (finished.Sub(start)))

}

func readDocs(ctx context.Context, db *mongo.Database) error {

	// find all the docs in the source collection
	c, err := db.Collection(source).Find(ctx, nil)
	if err != nil {
		return fmt.Errorf("Couldn't find any data: %v", err)
	}
	defer c.Close(ctx)

	// iterate through the collection, one document at a time.
	for c.Next(ctx) {
		// load that document into a bson.NewDocument object
		doc := bson.NewDocument()
		if err = c.Decode(doc); err != nil {
			return fmt.Errorf("can't decode a doc: %v", err)
		}

		// test to see if this document actually has a longitude value, some don't
		if doc.Lookup("Longitude") != nil {
			// and if does append a sub document to it made up from the values (VC - Value constructor) for Long and Lat.  Be careful to watch the order of Lon/Lat
			doc.Append(
				bson.EC.SubDocumentFromElements("coordinates",
					bson.EC.String("type", "Point"),
					bson.EC.ArrayFromElements("coordinates", bson.VC.Decimal128(doc.Lookup("Longitude").Decimal128()), bson.VC.Decimal128(doc.Lookup("Latitude").Decimal128()))))

			// now, delete the two fields that are no longer needed.
			doc.Delete("Longitude")
			doc.Delete("Latitude")

		} else {
			// if you want, uncomment the below to see which records have no lon/lat
			// fmt.Printf("--FAIL-FAIL-FAIL - %v has no things!\n", doc.Lookup("MMSI").Int32())
		}

		// once we have the newly shaped document, insert it into the DB
		insertDoc(ctx, db, doc)

	}

	if err = c.Err(); err != nil {
		return fmt.Errorf("all data couldn't be listed: %v", err)
	}
	return nil
}

func insertDoc(ctx context.Context, db *mongo.Database, doc *bson.Document) {
	// switch to the destination collection
	coll := db.Collection(dest)
	// insert the document into the collection
	_, err := coll.InsertOne(ctx, doc)

	if err != nil {
		fmt.Printf("Can' insert all the docs: %v, \n", err)
	}

}

/*

"coordinates":{
	"type":"Point",
	"coordinates":[-118.21171,33.77161]
	}

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
*/

/*
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
*/
