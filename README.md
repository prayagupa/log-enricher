log-enricher
---------------------

gets the xml body in inbound event and translates to json, don't laugh yet there more to call it enrichment

uses
----------

current purpose is to use it as a enricher on flume. so when flume consumes an event from whatever stream, this 
enricher will convert it to json and send it back to flume which then can send it to whatever outbound stream.

TODO
--------

make it more abstract
