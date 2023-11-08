// @param   data  table
// @return  .     table
.pipe.adhoc.appendSecuid: {[data]
  data: update secuid: 1 from data;
  :`sym`secuid xcols data
 };
