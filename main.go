package main

// resources:
// - https://www.compose.com/articles/mongodb-and-go-moving-on-from-mgo/
// - https://gitlab.com/wemgl/todocli/blob/master/main.go
// - https://godoc.org/github.com/mongodb/mongo-go-driver/bson

// todo:
// - work out what contexts are
// - deal with collections sizes that are not a multiple of batch size
// - DONE -- implement command line options
// - DONE -- make dropping collection optional
// - 2nd mode, update in place rather than copying to a new collection
// - make deletion of original lat/lon fields optional
// - delete the objectID so we don't get duplication errors when not dropping the dest collection on startup

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"
)

// grab a var for the connection string from the command line.
var uri = flag.String("uri", "mongodb://localhost", "The URI of the MongoDB instance you want to connect to")

// define database
var database = flag.String("db", "AIS", "The database to work in")

// define source and destination collections
var source = flag.String("source", "ais_10k", "the source collection")
var dest = flag.String("dest", "ais_10k_fix", "the destination collection")

// define the names of the fields containing the lat/lon coords
var lat = flag.String("fieldNameLat", "Latitude", "the name of the field in the source collection holding the Lattitude data")
var lon = flag.String("fieldNameLon", "Longitude", "the name of the field in the source collection holding the Longitude data")

// How many docs to read and write at once as part of a bulk insert
var batchSize = flag.Int("batchSize", 1000, "the number of documents to process in one batch")

// delete source lat/lons?
var deleteLatLonfields = flag.Bool("deleteLatLonfields", true, "Once the data is proccessed delete the original Lat Lon fields from the processed records, leaving only the GeoJSON")

// drop collection first?
var dropDestColl = flag.Bool("dropDestColl", true, "Before starting, drop the destination collection")

// preserve ObjectIDs?
var preserveObjectID = flag.Bool("preserveObjectID", false, "When inserting the new document, remove the objectID so the DB can assign a new one")

// how many docs have we got left to process
var docsLeft int

func main() {
	fmt.Println("\n---------------------------------\n--Lat/Lon conversion to GeoJSON |\n---------------------------------")

	// parse the flags
	flag.Parse()

	fmt.Printf("\n--uri: %v -- db is: %v", *uri, *database)
	fmt.Printf("\n--deleteLatLon: %v -- dropDestColl is: %v, pres_obj=%v\n", *deleteLatLonfields, *dropDestColl, *preserveObjectID)

	// create a context. (note to self, learn what a context is...)
	ctx := context.Background()

	// create a client for the DB
	client, err := mongo.NewClient(*uri)
	if err != nil {
		log.Fatal(err)
	}

	// Connect the client to the DB
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// select the database to use
	db := client.Database(*database)

	if *dropDestColl {
		// drop the destination collection (naughty! must deal with errors!)
		fmt.Printf("\n--Dropping Dest Collection:%v", *dest)
		_ = db.Collection(*dest).Drop(ctx, nil)
	} else {
		fmt.Printf("\n--NOT Dropping Collection:")
	}

	// note the start time
	start := time.Now()
	fmt.Printf("\n--Started at: %v\n", start)

	// find out how many docs we have to process
	countDocs(ctx, db)

	// start doing work
	processDocs(ctx, db, *batchSize)

	// note the end time
	finished := time.Now()
	fmt.Printf("\n--Finished at: %v\n", finished)
	fmt.Printf("--took: %v\n", (finished.Sub(start)))

}

func countDocs(ctx context.Context, db *mongo.Database) error {
	// count the number of documents in the source collection
	count, err := db.Collection(*source).Count(ctx, nil)
	if err != nil {
		return fmt.Errorf("Couldn't find any data: %v", err)
	}
	fmt.Printf("--there are %v docs in the collection\n\n", count)

	docsLeft = int(count)

	return nil
}

func processDocs(ctx context.Context, db *mongo.Database, batchSize int) error {

	fmt.Printf("--starting to process documents in batches of %v\n", batchSize)

	// find all the docs in the source collection
	c, err := db.Collection(*source).Find(ctx, nil)
	if err != nil {
		return fmt.Errorf("Couldn't find any data: %v", err)
	}
	defer c.Close(ctx)

	for docsLeft > 0 {
		//create a slice to hold the batch of documents in.
		var docs []interface{}
		for i := 0; i < batchSize; i++ {
			c.Next(ctx)
			// load that document into a bson.NewDocument object
			doc := bson.NewDocument()

			if err = c.Decode(doc); err != nil {
				return fmt.Errorf("can't decode a doc: %v", err)
			}

			// test to see if this document actually has a longitude value, some don't
			if doc.Lookup(*lon) != nil {
				// and if does append a sub document to it made up from the values (VC - Value constructor) for Long and Lat.  Be careful to watch the order of Lon/Lat
				doc.Append(
					bson.EC.SubDocumentFromElements("coordinates",
						bson.EC.String("type", "Point"),
						bson.EC.ArrayFromElements("coordinates", bson.VC.Decimal128(doc.Lookup("Longitude").Decimal128()), bson.VC.Decimal128(doc.Lookup("Latitude").Decimal128()))))

				// now, delete the two fields that are no longer needed (unless asked not to).
				if *deleteLatLonfields {
					doc.Delete(*lon)
					doc.Delete(*lat)
				}

				// remove the imported objectIDs so new ones can be created (unless told not to)
				if *preserveObjectID == false {
					doc.Delete("_id")
				}

			} else {
				// if you want, uncomment the below to see which records have no lon/lat
				//fmt.Printf("--FAIL-FAIL-FAIL - %v has no lat/lon!\n", doc.Lookup("MMSI").Int32())
			}

			docs = append(docs, doc)
		}

		docsLeft = docsLeft - batchSize
		fmt.Printf("%v Docs left to process\n", docsLeft)

		_, err = db.Collection(*dest).InsertMany(ctx, docs)
		if err = c.Err(); err != nil {
			return fmt.Errorf("could not insert: %v", err)
		}
	}

	if err = c.Err(); err != nil {
		return fmt.Errorf("all data couldn't be listed: %v", err)
	}

	return nil
}
