// @param   data  table
// @return  .     table
.pipe.adhoc.appendSecuidAndFilter: {[data]
  data: update secuid: 1 from data;
  :`sym`secuid xcols data
 };

// for date > 2004.06.25
.pipe.adhoc.loadLegacyTaqMaster: {[parPath; hdbPath; partition_; chunk]
  columns: `cusip`wi`sym`name`uot`ex`r;
  dataTypes: " SB SSI I S ";
  fixLengths: 1 9 1 15 15 60 1 54 2 2 6 84;
  table: flip columns!(dataTypes; fixLengths) 0: chunk;
  // ? append . to sym, may not be required
  table: update sym: { $[x ~ y; x; `$"." sv (0 , count y) _ x] }[string sym; string r]
    from table;
  table: update "TNBCIMPXTAW DZJKY" (0 | 1 + ex) from table;
  table: update uot: 0 10 25 50 100 1000i uot from table;
  table: select cusip, wi, sym, uot, ex, r from table where not cusip in ``0`000000000;
  if[count table;
    .log.Info ("upserting"; count table; "records");
    upsert[parPath] .Q.en[hdbPath] table
  ]
 };

.pipe.adhoc.loadLegacyTaqTrade: {[parPath; hdbPath; partition; chunk]
  columns: `time`ex`sym`s`cond`size`price`stop`corr;
  cfgMap: `s# (!) . flip (
    (2006.09.30; ("TCSS*IFBI "; 9 1 6 10  4 9 11 1 2 20)  );
    (2015.07.26; ("T CSS*IFBI ";9 3 1 6 10  4 9 11 1 2 51))
  );
  cfg: cfgMap @ partition;
  dataTypes: cfg[0];
  fixLengths: cfg[1];
  table: flip columns!(dataTypes; fixLengths) 0: chunk;
  // not sure what char to join sym and s
  table: update
      ex: ?[ex = "Q"; "T"; ex],
      sym: { $[count y; x; x , "." , y] } '[string sym; string s],
      price: price % 1e4,
      cond: { x (x in " @")?0b } each cond
    from table;
  if[count table;
    .log.Info ("upserting"; count table; "records");
    upsert[parPath] .Q.en[hdbPath] table
  ]
 };

.pipe.adhoc.loadLegacyTaqQuote: {[parPath; hdbPath; partition; chunk]
  columns: `time`ex`sym`s`cond`size`price`stop`corr;
  cfgMap: `s# (!) . flip (
    (0Nd       ; ("TCSSFIFIC "; 9 1 6 10 11 7 11 7 1 28)  );
    (2012.08.01; ("TCSSFIFIC "; 9 1 6 10 11 7 11 7 1 29)  );
    (2013.02.02; ("TCSSFIFIC "; 9 1 6 10 11 7 11 7 1 33)  );
    (2013.12.02; ("TCSSFIFIC "; 9 1 6 10 11 7 11 7 1 35)  );
    (2015.07.26; ("T CSSFIFIC ";9 3 1 6 10 11 7 11 7 1 77))
  );
  cfg: cfgMap @ partition;
  dataTypes: cfg[0];
  fixLengths: cfg[1];
  table: flip columns!(dataTypes; fixLengths) 0: chunk;
  // not sure what char to join sym and s
  table: update
      ex: ?[ex = "Q"; "T"; ex],
      sym: { $[count y; x; x , "." , y] } '[string sym; string s],
      bid: bid % 1e4,
      ask: ask % 1e4
    from table;
  if[count table;
    .log.Info ("upserting"; count table; "records");
    upsert[parPath] .Q.en[hdbPath] table
  ]
 };
