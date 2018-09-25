# MongoDB-LatLon2GeoJSON
A helper script to find lat/lon coordinates within documents in a MongoDB collection, and convert them to GeoJSON 


Very much a work in progress.





---------------------------------
--Lat/Lon conversion to GeoJSON |
---------------------------------
Usage of ./main:
  -batchSize int
        the number of documents to process in one batch (default 1000)
  -db string
        The database to work in (default "AIS")
  -deleteLatLonfields
        Once the data is proccessed delete the original Lat Lon fields from the processed records, leaving only the GeoJSON (default true)
  -dest string
        the destination collection (default "ais_10k_fix")
  -dropDestColl
        Before starting, drop the destination collection (default true)
  -fieldNameLat string
        the name of the field in the source collection holding the Lattitude data (default "Latitude")
  -fieldNameLon string
        the name of the field in the source collection holding the Longitude data (default "Longitude")
  -preserveObjectID
        When inserting the new document, remove the objectID so the DB can assign a new one
  -source string
        the source collection (default "ais_10k")
  -uri string
        The URI of the MongoDB instance you want to connect to (default "mongodb://localhost")
