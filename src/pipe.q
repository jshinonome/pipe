import {"./adhoc.q"};

.cli.Symbol[`hdbPath; `; "upsert hdb path"];
.cli.Symbol[`gzPath; `; "filepath"];
.cli.Date[`partition; 0Nd; "partition date"];
.cli.String[`delimiter; ","; "delimiter"];
.cli.Boolean[`debug; 0b; "debug mode"];
.cli.Boolean[`overwrite; 0b; "overwrite partition"];

.z.zd: 17 2 6;

.cli.Args: .cli.Parse[];

.pipe.cfgFiles: .path.Walk[.path.GetRelativePath { "../conf" }];

.pipe.readCfgFile: {[cfgPath]
  cfg: .j.k raze read0 cfgPath;
  cfg[`targetTable]: `$cfg[`targetTable];
  cfg[`sortBy]: `$cfg[`sortBy];
  cfg[`attribute]: `$cfg[`attribute];
  cfg[`adhoc]: `$cfg[`adhoc];
  cfg[`dropStart]: `int$cfg[`dropStart];
  cfg[`dropEnd]: `int$cfg[`dropEnd];
  if[count cfg[`columnMap];
    cfg[`columnMap]: update "C"$dataType, `$target from cfg[`columnMap]
  ];
  cfgName: (string first ` vs last ` vs cfgPath);
  startDate: "D"$-8 # cfgName;
  pattern: "*" , (-9 _ cfgName) , "*";
  (pattern; startDate; cfg)
 };

.pipe.cfgMap: `pattern`startDate xasc
  2!flip `pattern`startDate`cfg!flip .pipe.readCfgFile each .pipe.cfgFiles `file;

.pipe.load: {[gzPath; hdbPath; partition; delimiter; overwrite]
  .log.Info ("loading file"; gzPath; "to"; hdbPath);
  startTime: .z.P;
  cfg: last exec cfg from .pipe.cfgMap where gzPath like/: pattern, partition >= startDate;
  table: cfg `targetTable;
  columnMap: cfg `columnMap;
  sortBy: cfg `sortBy;
  attribute: cfg `attribute;
  adhoc: cfg `adhoc;
  dropStart: cfg `dropStart;
  dropEnd: cfg `dropEnd;
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
  $[
    count columnMap;
      .Q.fpn[
        .pipe.loadChunk[
          parPath;
          hdbPath;
          columns;
          dataTypes;
          first delimiter;
          adhoc
        ];
        hsym `$namedPipe;
        5000000
      ];
      .Q.fpn[(value adhoc)[parPath; hdbPath; partition]; hsym `$namedPipe; 5000000]
  ];
  .pipe.remove[namedPipe];
  .log.Info ("time used"; .z.P - startTime);
  .pipe.post[
    parPath;
    sortBy;
    attribute;
    dropStart;
    dropEnd
  ]
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

.pipe.loadChunk: {[parPath; hdbPath; columns; dataTypes; delimiter; adhoc; chunk]
  table: flip columns!(dataTypes; delimiter) 0: chunk;
  if[not null adhoc;
    table: (value adhoc) table
  ];
  if[count table;
    .log.Info ("upserting"; count table; "records");
    upsert[parPath] .Q.en[hdbPath] table
  ]
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
    .pipe.drop[parPath; ; dropStart; neg abs dropEnd] '[cols parPath]
  ];
  .pipe.applyAttribute[parPath] '[key attribute; value attribute]
 };

.pipe.getColumns: {[gzPath; delimiter]
  :delimiter vs first system "zcat " , (1 _ string gzPath) , " | head -1"
 };

if[11h = not type key .cli.Args `hdbPath;
  .log.Error ("no such directory - " , string .cli.Args `hdbPath);
  exit 1
 ];

if[-11h = not type key .cli.Args `gzPath;
  .log.Error ("no such file - " , string .cli.Args `gzPath);
  exit 1
 ];

if[null .cli.Args `partition;
  .log.Error ("requires non-null partition");
  exit 1
 ];

if[not .cli.Args `debug;
  .Q.trp[
    value;
    (.pipe.load , .cli.Args `gzPath`hdbPath`partition`delimiter`overwrite);
    {
      .log.Error "failed to load with error - " , x;
      "\n  backtrace:";
      .Q.sbt y;
      exit 1
    }
  ];
  exit 0
 ];

.pipe.load . .cli.Args `gzPath`hdbPath`partition`delimiter`overwrite;
