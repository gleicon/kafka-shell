## Building

	$ make all
	$ make install

## Usage
### ktail

	tail a topic
	$ ktail -s broker-1,broker-2,broker-3 topic.name

	help
	$ ktail -h

	to the last offset (most recent)
	$ ktail -n -1 -s broker-1,broker-2,broker-3 topic.name

	to the first offset
	$ ktail -n 0 -s broker-1,broker-2,broker-3 topic.name

	arbitrary offset
	$ ktail -n <offset> -s broker-1,broker-2,broker-3 topic.name

