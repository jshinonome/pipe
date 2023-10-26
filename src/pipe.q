.cli.Symbol[`hdbPath; `; "upsert hdb path"];
.cli.Symbol[`gzPath; `; "filepath"];
.cli.Date[`partition; 0Nd; "partition date"];
.cli.String[`delimiter; ","; "delimiter"];
.cli.Boolean[`debug; 0b; "debug mode"];
.cli.Boolean[`overwrite; 0b; "overwrite partition"];
.cli.Int[`dropStart; 0; "drop records from start"];
.cli.Int[`dropEnd; 0; "drop records from end"];

.z.zd: 17 2 6;

.cli.Args: .cli.Parse[];

.pipe.cfgFiles: .path.Walk[.path.GetRelativePath { "../conf" }];

.pipe.readCfgFile: {[cfgPath]
  cfg: `$.j.k raze read0 cfgPath;
  cfg[`columnMap]: update string source, "C"$string dataType from cfg[`columnMap];
  pattern: "*" , (string first ` vs last ` vs cfgPath) , "*";
  (pattern; cfg)
 };

.pipe.cfgMap: (!) . flip .pipe.readCfgFile each .pipe.cfgFiles `file;

.pipe.load: {[gzPath; hdbPath; partition; delimiter; overwrite; dropStart; dropEnd]
  .log.Info ("loading file"; gzPath; "to"; hdbPath);
  startTime: .z.P;
  cfg: first (value .pipe.cfgMap) where gzPath like/: key .pipe.cfgMap;
  table: cfg `targetTable;
  columnMap: cfg `columnMap;
  sortBy: cfg `sortBy;
  attribute: cfg `attribute;
  parPath: .Q.dd[.Q.par[hdbPath; partition; table]; `];
  if[overwrite;
    .pipe.removePartition parPath
  ];
  columns: exec target from columnMap where not null target;
  .log.Info ("loading columns "; "," sv string columns);
  dataTypes: (exec source!dataType from columnMap where not null target)
    .pipe.getColumns[gzPath; first delimiter];
  .log.Info ("loading data to partition"; parPath);
  namedPipe: "/tmp/" , (string .z.i) , ".pipe";
  .pipe.make[gzPath; namedPipe];
  .Q.fpn[
    .pipe.loadChunk[parPath; hdbPath; columns; dataTypes; first delimiter];
    hsym `$namedPipe;
    5000000
  ];
  .pipe.remove[namedPipe];
  .log.Info ("time used"; .z.P - startTime);
  .pipe.post[parPath; sortBy; attribute; dropStart; dropEnd]
 };

.pipe.make: {[gzPath; namedPipe]
  .log.Info ("make name pipe"; namedPipe);
  system "mkfifo " , namedPipe;
  system "gzip -cd " , (1 _ string gzPath) , " > " , (namedPipe) , " &"
 };

.pipe.remove: {[namedPipe] system "rm " , namedPipe };

.pipe.removePartition: {[parPath]
  .log.Info ("remove partition"; parPath);
  system "rm -rf " , 1 _ string parPath
 };

.pipe.loadChunk: {[parPath; hdbPath; columns; dataTypes; delimiter; chunk]
  table: flip columns!(dataTypes; delimiter) 0: chunk;
  .log.Info ("upserting"; count table; "records");
  upsert[parPath] .Q.en[hdbPath] table
 };

.pipe.applyAttribute: {[parPath; column; attribute]
  .log.Info ("applying attribute"; attribute; "to"; column);
  .[` sv parPath , column; (); attribute #];
  .log.Info ("finished applying attribute"; attribute; "to"; column)
 };

.pipe.drop: {[parPath; column; dropStart; dropEnd]
  columnPath: ` sv parPath , column;
  columnPath set dropEnd _ dropStart _ get columnPath
 };

.pipe.post: {[parPath; sortBy; attribute; dropStart; dropEnd]
  if[count sortBy;
    sortBy xasc parPath
  ];
  if[dropStart | abs dropEnd;
    .log.Info ("drop"; dropStart; "records from start");
    .log.Info ("drop"; dropEnd; "records from end");
    .pipe.drop[parPath; ; dropStart; neg abs dropEnd]'[cols parPath]
  ];
  .pipe.applyAttribute[parPath] '[key attribute; value attribute]
 };

.pipe.getColumns: {[gzPath; delimiter]
  :delimiter vs first system "zcat " , (1 _ string gzPath) , " | head -1"
 };

if[11h=not type key .cli.Args`hdbPath;
  .log.Error("no such directory - ", string .cli.Args`hdbPath);
  exit 1
 ];

if[-11h=not type key .cli.Args`gzPath;
  .log.Error("no such file - ", string .cli.Args`gzPath);
  exit 1
 ];

if[null .cli.Args`partition;
  .log.Error("requires non-null partition");
  exit 1
 ];

if[not .cli.Args`debug;
  .Q.trp[
    value;
    (.pipe.load , .cli.Args `gzPath`hdbPath`partition`delimiter`overwrite`dropStart`dropEnd);
    {
      .log.Error "failed to load with error - ", x;"\n  backtrace:";.Q.sbt y;
      exit 1
    }];
  exit 0
 ];

.pipe.load . .cli.Args `gzPath`hdbPath`partition`delimiter`overwrite`dropStart`dropEnd;
