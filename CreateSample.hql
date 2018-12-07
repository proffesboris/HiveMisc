

drop table if exists custom_cb_k7m_aux.det_crit_lim;

CREATE TABLE `t_team_k7m_aux_d.det_crit_lim`(

  `crit` string,

  `up` int,

  `low` double)

row format delimited

fields terminated by '\t'

stored as textfile;

alter table custom_cb_k7m_aux.det_crit_lim SET SERDEPROPERTIES("serialization.encoding"='Windows-1251');

load data local inpath 'det_crit_lim.data' overwrite into table custom_cb_k7m_aux.det_crit_lim;
 
