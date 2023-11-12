# pipe

## Loader

### pipe.q

```
  -hdbPath     symbol     upsert hdb path
  -gzPath      symbol     filepath
  -partition   date       partition date
  -delimiter   string     delimiter
  -debug       boolean    debug mode
  -overwrite   boolean    overwrite partition
```

e.g. using ktrl to run the script

```
ktrl --start --process pipe --profile q4 --kargs " -gzPath :taq/REDUCED_EQY_US_ALL_REF_MASTER_20230703.gz -hdbPath :/tmp/hdb -partition 20230703 -delimiter '|' -overwrite 1b -dropStart 1 -dropEnd 1"
```

update configurations in `conf` directory, empty target will be skipped. This shall support different versions of files, as it checks the header of gz files and use the header to extract columns.

### taq_loader.py

#### Usage

```
usage: taq_loader.py [-h] [-t THREAD] -d DIR --hdb HDB

TAQ Loader CLI

options:
  -h, --help            show this help message and exit
  -t THREAD, --thread THREAD
                        thread number
  -d DIR, --dir DIR     directory that stores taq gz files
  --hdb HDB             q hdb directory
```

taq_loader will cached processed gz files to `~/.cache/pipe/loaded`, so it can be trigger daily.

.e.g

```
python py/taq_loader.py --dir taq --hdb /tmp/hdb
```

Log Files: `/tmp/ForkPoolWorker-*`
