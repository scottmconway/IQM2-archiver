# IQM2 Archiver
A tool for archiving resolution data from IQM2 instances for civic engagement. Inspiration taken from [CityScrapers](https://cityscrapers.org/).

## Requirements
* python3
* see requirements.txt

## Configuration Setup
See `config.json.example` for an example config.
|Parameter Name|Type|Description|
|-|-|-|
|`iqm_root_url`|`str`|The root of the IQM2 instance to interact with|
|`database_engine_uri`|`str`|A database engine URI to pass to SqlAlchemy|
|`resolution_range`|`list[int]`|A list of integer values that can be read as a `range` object, for the resolution IDs that you want to scrape|

## `iqm_resolution_archiver.py`
### Arguments
|Short Name|Long Name|Type|Description|
|-|-|-|-|
|`-c`|`--config`|`str`|Path to config file - defaults to `./config.json`|

### Limitations
IQM2 is _full_ of custom fields and inconsistent rendering of information. This script has been tested on a number of IQM2 instances with varying configurations, but issues due to the use of custom fields may arise nonetheless. Please open an issue if you have issues on a given IQM2 instance.

All known current limitations can be found [here](https://github.com/scottmconway/iqm2_archiver/issues?q=is%3Aopen%20label%3Alimitation).
