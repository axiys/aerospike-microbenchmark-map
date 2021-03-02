# Aerospike: Microbenchmark - Map
This microbenchmark will test the put and delete of a map type bin.

Creates multi-threaded workers that will:
* create a bin holding a map
* randomly put key-values into map with random keys key0 ... key99 and values are randomly generated base64 encoded byte arrays
* check that the map key exists
* check that the map value is not null

## Dependencies
* Maven
* Java 8
* Aerospike Client 4.4.6
* Aerospike Server CE 4.8.0.6
* Docker Compose

## Usage
```
docker-compose up -d
mvn install
```
