set mapreduce.map.memory.mb=34816;
set mapreduce.map.java.opts=-Xmx27854m;
set mapreduce.reduce.memory.mb=34816;
set mapreduce.reduce.java.opts=-Xmx27854m;
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.cbo.enable=true;
set hive.compute.query.using.stats=true;
set hive.stats.fetch.column.stats=true;
set hive.stats.fetch.partition.stats=true;
set mapred.compress.map.output=true;
set mapred.output.compress=true;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;


set mapreduce.job.queuename=root.smart_pre_approval;
set mapreduce.map.memory.mb=131072;
set mapreduce.map.java.opts=-Xmx104858m;
set mapreduce.reduce.memory.mb=131072;
set mapreduce.reduce.java.opts=-Xmx104858m;
set yarn.scheduler.maximum-allocation-mb=131072;
set mapreduce.map.output.compress;
set mapreduce.map.output.compress.codec;
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
--analyze table custom_cb_preapproval.z_main_docum_filt_partition compute statistics for columns c_kl_kt_1_2, c_filial;
--analyze table internal_eks_ibs.z_ac_fin compute statistics for columns id;
--analyze table internal_eks_ibs.z_client compute statistics for columns id;
--analyze table internal_eks_ibs.z_branch compute statistics for columns id;
--analyze table internal_eks_ibs.z_name_paydoc compute statistics for columns id;
--analyze table internal_eks_ibs.z_ft_money compute statistics for columns id;
set hive.cbo.enable=true;
set hive.compute.query.using.stats=true;
set hive.stats.fetch.column.stats=true;
set hive.stats.fetch.partition.stats=true;
explain create table custom_cb_preapproval.smart_src_bor_new_new
STORED AS TEXTFILE
as SELECT
              acc.customer_inn, 
              acc.customer_kpp, 
              nvl(md.c_kl_kt_2_inn, cus2.c_inn) AS customer_2_inn, 
              nvl(md.c_kl_kt_2_kpp, cus2.c_kpp) AS customer_2_kpp,
              md.id,
              md.c_local_code BRANCH_CD,
              cast(concat(date_sub(md.c_date_prov, 0), ' 00:00:00') as TIMESTAMP) DT,
              (case  when md.c_code = 'БЕЗН_ПЛ_ПОРУЧ'    then 1
              when md.c_code = 'БЕЗН_БАНК_ОРД'    then 2
              when md.c_code = 'БЕЗН_ПЛ_ТРЕБ'     then 3
              when md.c_code = 'КАСС_ОБЪЯВ_ВЗНОС' then 4
              when md.c_code = 'БЕЗН_МЕМО_ОРД'    then 5
              when md.c_code = 'БЕЗН_ПЛ_ОР'       then 6
              when md.c_code = 'КАСС_ЧЕК_ВЫДАЧА'  then 7
              when md.c_code = 'БЕЗН_ИНКАСС_ПОР'  then 8
              else 0 end) as TYPE_CODE,
             'NULL' SUBTYPE_CODE,
             round(document_amt*(case when acc.c_code_iso = '810' then 1
             when acc.c_code_iso = '840' then 55
             when acc.c_code_iso = '978' then 61
             else 1 end) ) as val,
             document_amt,
             acc.c_code_iso,
             'NULL' cp_inn,
              md.document_desc, 
              md.c_num_kt,
              md.c_num_dt,
             md.c_code,
              md.c_kl_dt_2_3 bank_2_id, 
              md.c_kl_dt_2_2 customer_2_nm
              FROM custom_cb_preapproval.z_main_docum_filt_partition md
             left JOIN custom_cb_preapproval.z_ac_fin_z_client_z_ft_money acc
            ON (md.c_date_prov_year_mnth in ('2016-06','2016-07','2016-08','2016-09','2016-10','2016-11','2016-12','2017-01','2017-02','2017-03','2017-04','2017-05') and md.c_kl_kt_1_2 = acc.id)
            left join custom_cb_preapproval.z_client cus2
            on cus2.id = md.c_kl_kt_1_1;
			
			
			
			
set mapreduce.map.memory.mb=34816;
set mapreduce.map.java.opts=-Xmx27854m;
set mapreduce.reduce.memory.mb=34816;
set mapreduce.reduce.java.opts=-Xmx27854m;
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.cbo.enable=true;
set hive.compute.query.using.stats=true;
set hive.stats.fetch.column.stats=true;
set hive.stats.fetch.partition.stats=true;
set mapred.compress.map.output=true;
set mapred.output.compress=true;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;

----------------------------------------------------------



drop table if exists custom_cb_preapproval.z_main_docum_filt;
drop table if exists custom_cb_preapproval.z_main_docum_filt_partition;
CREATE TABLE custom_cb_preapproval.z_main_docum_filt 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
as SELECT 
         SUBSTR(md.c_date_prov,1,7) c_date_prov_year_mnth,
         md1.c_valuta,
         md1.c_valuta_po,
         md1.c_date_prov,
         md1.c_filial,
         md1.c_kl_dt_1_1,
         md1.c_kl_dt_1_2,
         md1.c_kl_dt_2_2,
        ` md1.c_kl_dt_2_3,
         md1.c_kl_dt_2_inn,
         md1.c_kl_dt_2_kpp,
         md1.c_kl_kt_1_1,
         md1.c_kl_kt_1_2,
         md1.c_kl_kt_2_2,
         md1.c_kl_kt_2_3,
         md1.c_kl_kt_2_inn,
         md1.c_kl_kt_2_kpp,
         md1.c_nazn,
         md1.c_num_dt,
         md1.c_num_kt,
         md1.c_sum,
         md1.c_vid_doc,
         md1.id,
         md1.state_id,
         md1.c_num_kt IS NOT NULL AND NOT (substring(md1.c_num_kt,1,3) in ('301','302','303','304','706','202','603')) kt_is_good,
		 md1.c_num_dt IS NOT NULL AND NOT (substring(md1.c_num_dt,1,3) in ('301','302','303','304','706','202','603')) dt_is_good
		 WHERE upper(md1.state_id) = 'PROV' from internal_eks_ibs.z_main_docum md1;     
DROP TABLE IF EXISTS custom_cb_preapproval.pre_md;		 
create table custom_cb_preapproval.pre_md(
c_valuta,
c_valuta_po,
c_date_prov	timestamp,
c_filial decimal(38,12),
c_kl_dt_1_1	decimal(38,12),
c_kl_dt_1_2	decimal(38,12),
c_kl_dt_2_2	string,
c_kl_dt_2_3	decimal(38,12),
c_kl_dt_2_inn string,
c_kl_dt_2_kpp string,
c_kl_kt_1_1 decimal(38,12),
c_kl_kt_1_2	decimal(38,12),
c_kl_kt_2_2,
c_kl_kt_2_3,
c_kl_kt_2_inn string,
c_kl_kt_2_kpp string,
c_nazn,
c_num_dt string,
c_num_kt string,
c_sum,
c_vid_doc decimal(38,12),
id decimal(38,12),	
state_id string,
kt_is_good,
dt_is_good
)
partitioned by(c_date_prov_year_mnth string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;
insert into table custom_cb_preapproval.pre_md
partition(c_date_prov_year_mnth)
select   c_valuta,
         c_valuta_po,
         c_date_prov,
         c_filial,
         c_kl_dt_1_1,
         c_kl_dt_1_2,
         c_kl_dt_2_2,
         c_kl_dt_2_3,
         c_kl_dt_2_inn,
         c_kl_dt_2_kpp,
         c_kl_kt_1_1,
         c_kl_kt_1_2,
         c_kl_kt_2_2,
         c_kl_kt_2_3,
         c_kl_kt_2_inn,
         c_kl_kt_2_kpp,
         c_nazn,
         c_num_dt,
         c_num_kt,
         c_sum,
         c_vid_doc,
         id,
         state_id,
         kt_is_good,
		 dt_is_good,
c_date_prov_year_mnth
from custom_cb_preapproval.z_main_docum_filt
where c_date_prov_year_mnth in ('2016-04','2016-05','2016-06','2016-07','2016-08', '2016-09', '2016-10', '2016-11', '2016-12','2017-01', '2017-02', '2017-03')
--distribute by c_date_prov_year_mnth;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------




 ------------------------- общая часть ---------------------- 
 
 
 
 /*
 
 
 DROP TABLE IF EXISTS custom_cb_preapproval.pre_md;

 create table custom_cb_preapproval.pre_md
 stored as parquet
 as SELECT md1.c_valuta,
         md1.c_valuta_po,
         md1.c_date_prov,
         md1.c_filial,
         md1.c_kl_dt_1_1,
         md1.c_kl_dt_1_2,
         md1.c_kl_dt_2_2,
        ` md1.c_kl_dt_2_3,
         md1.c_kl_dt_2_inn,
         md1.c_kl_dt_2_kpp,
         md1.c_kl_kt_1_1,
         md1.c_kl_kt_1_2,
         md1.c_kl_kt_2_2,
         md1.c_kl_kt_2_3,
         md1.c_kl_kt_2_inn,
         md1.c_kl_kt_2_kpp,
         md1.c_nazn,
         md1.c_num_dt,
         md1.c_num_kt,
         md1.c_sum,
         md1.c_vid_doc,
         md1.id,
         md1.state_id,
         md1.c_num_kt IS NOT NULL AND NOT (substring(md1.c_num_kt,1,3) in ('301','302','303','304','706','202','603')) kt_is_good,
		 md1.c_num_dt IS NOT NULL AND NOT (substring(md1.c_num_dt,1,3) in ('301','302','303','304','706','202','603')) dt_is_good 
		 FROM internal_eks_ibs.z_main_docum as md1 WHERE upper(md1.state_id) = 'PROV';

*/		 
		 
		 
		 
		 
 --первый шаг но с промежуточной таблицей 
DROP TABLE IF EXISTS custom_cb_preapproval.z_main_docum_short_part;

 CREATE TABLE custom_cb_preapproval.z_main_docum_short_part
 ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
 AS SELECT /*+ mapjoin(bran, pd) */ md.c_valuta,
         md.c_valuta_po,
         md.c_date_prov,
         md.c_kl_dt_1_1,
         md.c_kl_dt_1_2,
         md.c_kl_dt_2_2,
         md.c_kl_dt_2_3,
         md.c_kl_dt_2_inn,
         md.c_kl_dt_2_kpp,
         md.c_kl_kt_1_1,
         md.c_kl_kt_1_2,
         md.c_kl_kt_2_2,
         md.c_kl_kt_2_3,
         md.c_kl_kt_2_inn,
         md.c_kl_kt_2_kpp,
         md.c_nazn,
         md.c_num_dt,
         md.c_num_kt,
         md.c_sum,
         md.id,
         md.state_id,
         bran.c_local_code,
         pd.c_code,
         SUBSTR(md.c_date_prov,1,7) c_date_prov_year_mnth,
         (case  WHEN md.c_kl_dt_2_inn IS NOT NULL  AND md.c_kl_kt_2_inn IS NULL THEN
    '2'
    WHEN md.c_kl_dt_2_inn IS NULL
        AND md.c_kl_kt_2_inn IS NULL THEN
    '1'
    WHEN md.c_kl_dt_2_inn IS NOT NULL
        AND md.c_kl_kt_2_inn IS NOT NULL THEN
    '4'
    WHEN md.c_kl_dt_2_inn IS NULL
        AND md.c_kl_kt_2_inn IS NOT NULL THEN
    '3'
    ELSE '0' end) AS nulls, (case
    WHEN kt_is_good
        AND NOT dt_is_good THEN
    '2'
    WHEN NOT kt_is_good
        AND NOT dt_is_good THEN
    '1'
    WHEN kt_is_good
        AND dt_is_good THEN
    '4'
    WHEN NOT kt_is_good
        AND dt_is_good THEN
    '3'
    ELSE '0' end) AS goods
FROM custom_cb_preapproval.pre_md AS md
LEFT JOIN internal_eks_ibs.z_branch bran
    ON (md.c_filial = bran.id)
LEFT JOIN internal_eks_ibs.z_name_paydoc pd
    ON (md.c_vid_doc = pd.id);

 DROP TABLE IF EXISTS custom_cb_preapproval.z_client_short;

 CREATE TABLE custom_cb_preapproval.z_client_short 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
 AS SELECT cl.id,
         cl.c_kpp,
         cl.c_inn
FROM internal_eks_ibs.z_client AS cl
WHERE cl.c_inn IS NOT NULL;

 DROP TABLE IF EXISTS custom_cb_preapproval.z_ac_fin_short;

 CREATE TABLE custom_cb_preapproval.z_ac_fin_short 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
 AS SELECT ac.id,
         ac.c_fintool,
         ac.c_client_v
FROM internal_eks_ibs.z_ac_fin AS ac
WHERE ac.c_client_v IS NOT NULL
        AND ac.c_fintool IS NOT NULL;

 DROP TABLE IF EXISTS custom_cb_preapproval.z3;

 CREATE TABLE custom_cb_preapproval.z3 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
 AS SELECT /*+ mapjoin(ft) */ cl.c_inn,
         cl.c_kpp,
         ac.id,
         ft.c_code_iso,
         ac.c_fintool
FROM custom_cb_preapproval.z_ac_fin_short AS ac
LEFT JOIN custom_cb_preapproval.z_client_short AS cl
    ON ac.c_client_v = cl.id
LEFT JOIN internal_eks_ibs.z_ft_money AS ft
    ON (ac.c_fintool = ft.id);



 ------------------------- общая часть ---------------------- 
   DROP TABLE IF EXISTS custom_cb_preapproval.z3f_giant;

 CREATE TABLE custom_cb_preapproval.z3f_giant 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
 as SELECT z.id,
         z.c_inn,
         z.c_kpp,
         z.c_code_iso,
         z.c_fintool FROM custom_cb_preapproval.z3 AS z WHERE z.c_inn IN (SELECT с_inn from custom_cb_preapproval.all_clients_clean);

 DROP TABLE IF EXISTS custom_cb_preapproval.dbj;

 CREATE TABLE custom_cb_preapproval.dbj 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
 as SELECT db_bad.c_valuta,
         db_bad.c_valuta_po,
         db_bad.c_date_prov,
         db_bad.c_kl_dt_1_1,
         db_bad.c_kl_dt_1_2,
         db_bad.c_kl_dt_2_2,
         db_bad.c_kl_dt_2_3,
         db_bad.c_kl_dt_2_inn,
         db_bad.c_kl_dt_2_kpp,
         db_bad.c_kl_kt_1_1,
         db_bad.c_kl_kt_1_2,
         db_bad.c_kl_kt_2_2,
         db_bad.c_kl_kt_2_3,
         db_bad.c_kl_kt_2_inn,
         db_bad.c_kl_kt_2_kpp,
         db_bad.c_nazn,
         db_bad.c_num_dt,
         db_bad.c_num_kt,
         db_bad.c_sum,
         db_bad.id,
         db_bad.state_id,
         db_bad.c_date_prov_year_mnth,
         db_bad.nulls,
         db_bad.goods,
         db_bad.c_local_code,
         db_bad.c_code,
         zthree.c_inn,
         zthree.c_kpp,
         zthree.c_code_iso,
         zthree.c_fintool FROM (SELECT mdsp.c_valuta,
         mdsp.c_valuta_po,
         mdsp.c_date_prov,
         mdsp.c_kl_dt_1_1,
         mdsp.c_kl_dt_1_2,
         mdsp.c_kl_dt_2_2,
         mdsp.c_kl_dt_2_3,
         mdsp.c_kl_dt_2_inn,
         mdsp.c_kl_dt_2_kpp,
         mdsp.c_kl_kt_1_1,
         mdsp.c_kl_kt_1_2,
         mdsp.c_kl_kt_2_2,
         mdsp.c_kl_kt_2_3,
         mdsp.c_kl_kt_2_inn,
         mdsp.c_kl_kt_2_kpp,
         mdsp.c_nazn,
         mdsp.c_num_dt,
         mdsp.c_num_kt,
         mdsp.c_sum,
         mdsp.id,
         mdsp.state_id,
         mdsp.c_date_prov_year_mnth,
         mdsp.nulls,
         mdsp.goods,
         mdsp.c_local_code,
         mdsp.c_code FROM custom_cb_preapproval.z_main_docum_short_part AS mdsp 
   -- так как выяснилось что в ZMD банк там стоит ошибочно. 
   WHERE (mdsp.goods = '3' OR mdsp.goods = '4') AND c_date_prov_year_mnth IN ('2016-04','2016-05','2016-06','2016-07','2016-08', '2016-09', '2016-10', '2016-11', '2016-12','2017-01', '2017-02', '2017-03') ) AS db_bad JOIN custom_cb_preapproval.z3f_giant AS zthree ON (zthree.id = db_bad.c_kl_dt_1_2);

 DROP TABLE IF EXISTS custom_cb_preapproval.crj;

 CREATE TABLE custom_cb_preapproval.crj 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
 as SELECT db_bad.c_valuta,
         db_bad.c_valuta_po,
         db_bad.c_date_prov,
         db_bad.c_kl_dt_1_1,
         db_bad.c_kl_dt_1_2,
         db_bad.c_kl_dt_2_2,
         db_bad.c_kl_dt_2_3,
         db_bad.c_kl_dt_2_inn,
         db_bad.c_kl_dt_2_kpp,
         db_bad.c_kl_kt_1_1,
         db_bad.c_kl_kt_1_2,
         db_bad.c_kl_kt_2_2,
         db_bad.c_kl_kt_2_3,
         db_bad.c_kl_kt_2_inn,
         db_bad.c_kl_kt_2_kpp,
         db_bad.c_nazn,
         db_bad.c_num_dt,
         db_bad.c_num_kt,
         db_bad.c_sum,
         db_bad.id,
         db_bad.state_id,
         db_bad.c_date_prov_year_mnth,
         db_bad.nulls,
         db_bad.goods,
         db_bad.c_local_code,
         db_bad.c_code,
         zthree.c_inn,
         zthree.c_kpp,
         zthree.c_code_iso,
         zthree.c_fintool FROM (SELECT mdsp.c_valuta,
         mdsp.c_valuta_po,
         mdsp.c_date_prov,
         mdsp.c_kl_dt_1_1,
         mdsp.c_kl_dt_1_2,
         mdsp.c_kl_dt_2_2,
         mdsp.c_kl_dt_2_3,
         mdsp.c_kl_dt_2_inn,
         mdsp.c_kl_dt_2_kpp,
         mdsp.c_kl_kt_1_1,
         mdsp.c_kl_kt_1_2,
         mdsp.c_kl_kt_2_2,
         mdsp.c_kl_kt_2_3,
         mdsp.c_kl_kt_2_inn,
         mdsp.c_kl_kt_2_kpp,
         mdsp.c_nazn,
         mdsp.c_num_dt,
         mdsp.c_num_kt,
         mdsp.c_sum,
         mdsp.id,
         mdsp.state_id,
         mdsp.c_date_prov_year_mnth,
         mdsp.nulls,
         mdsp.goods,
         mdsp.c_local_code,
         mdsp.c_code FROM custom_cb_preapproval.z_main_docum_short_part AS mdsp WHERE (mdsp.goods = '2' OR mdsp.goods = '4') AND c_date_prov_year_mnth IN ('2016-04','2016-05','2016-06','2016-07','2016-08', '2016-09', '2016-10', '2016-11', '2016-12','2017-01', '2017-02', '2017-03') ) AS db_bad JOIN custom_cb_preapproval.z3f_giant AS zthree ON (zthree.id = db_bad.c_kl_kt_1_2);

 --делаем настоящие фичи 
  DROP TABLE IF EXISTS custom_cb_preapproval.full_giant;

 CREATE TABLE custom_cb_preapproval.full_giant 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
 AS SELECT crj.*,
         'NEW' AS source, 'CR' AS dbcr
FROM custom_cb_preapproval.crj AS crj
UNION
ALL 
SELECT dbj.*,
         'NEW' AS source, 'DB' AS dbcr
FROM custom_cb_preapproval.dbj AS dbj;

 --тут аккуратно, не перетереть бы старые данные 
 --чтобы сработал код вычисления trtype, таблица должна иметь имя newfull 
drop table if EXISTS custom_cb_preapproval.newfull;

 alter table custom_cb_preapproval.full_giant rename to custom_cb_preapproval.newfull;


 
 
 
 
 
 





