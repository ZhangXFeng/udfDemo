 create temporary function many2One as 'io.transwarp.udf.Many2One';

 create temporary function one2many as 'io.transwarp.udf.One2many';

 create temporary function ReverseStr as 'io.transwarp.udf.ReverseStr';

create temporary function geohash as 'io.transwarp.udf.Location2Geohash';

--select many2one(i) from rownum_inceptor limit 1;
--select one2many(visitdate,'-') from uservisits_copy limit 10;
--select ReverseStr(visitdate) from uservisits_copy limit 10;
--select key ,geohash(lat,lon) from hq_ais_history_data_es limit 10;

