[![Build Status](https://travis-ci.org/pmezard/apec.png?branch=master)](https://travis-ci.org/pmezard/apec)

# What is it?

The APEC is an official French job board for middle management/executive jobs:

  [http://apec.fr](http://apec.fr)

This project is an experiment on data collection, geocoding, indexing, analysis and visualization, based on APEC job offers. The result is interesting because:
 * It provides real data to play with, with all its flaws (typo, mismatched fields, unsanitized inputs, etc.) 
 * It highlights trends, in recruiting cycles, in popular technologies, etc.
 * It has to run on a Raspberry Pi 2 which brings amusing technical constraints.

Live demo:

  [http://mezard.eu/apec/](http://mezard.eu/apec/)


# Usage

OpenCage geocoder is used to locate the job offers, you can get an API key from
[http://geocoder.opencagedata.com/](http://geocoder.opencagedata.com/). Then
put it in $APEC_GEOCODING_KEY so apec command can use it automatically.

```
# Crawl job offers in Finistère (west of Brittany)
$ apec crawl --location=29

# Index and geocode them
$ APEC_GEOCODING_KEY=YOUR_OPENCAGE_API_KEY apec index

# Start the web server on :8081
$ apec web
```

All commands can be listed with:
```
$ apec
```
