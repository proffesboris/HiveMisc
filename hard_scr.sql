drop table eks_transact_20161223_dan purge;

create table eks_transact_20161223_dan
(customer_inn  varchar(1000),
 customer_kpp  varchar(1000),
 account_ccy_cd  varchar(1000), 
 document_dt varchar(1000),
 acc_corr_nm   varchar(1000), 
 bank_2_bik varchar(1000),
 customer_2_nm varchar(1000),
 customer_2_inn varchar(1000),
 customer_2_kpp varchar(1000),
 dbcr_cd varchar(1000),
 document_amt varchar(1000),
 document_desc   varchar(4000),
 document_type_cd  varchar(1000),
 branch_cd varchar(1000));


drop table eks_to_calc_161223_dan purge;
drop table eks_to_promdm_dan1_2_part1 purge;
drop table eks_to_dan1_cust1_part1_2610 purge;
drop table eks_to_promdm_dan1_sup1_part1 purge;
drop table eks_to_promdm_dan1_sup2_part1 purge;
drop table eks_to_dan1_cust2_part1_2610 purge;
drop table eks_to_promdm_dan1_fin purge;

create table eks_to_calc_161223_dan as
select CUSTOMER_INN as inn
       ,BRANCH_CD as tb 
       ,DOCUMENT_DT as dt
       ,(case when DBCR_CD = 'DB' then 1
              when DBCR_CD = 'CR' then 0
              else null end) as debit         
       ,(case when DOCUMENT_TYPE_CD = 'БЕЗН_ПЛ_ПОРУЧ' then 1
              when DOCUMENT_TYPE_CD = 'БЕЗН_БАНК_ОРД' then 2
              when DOCUMENT_TYPE_CD = 'БЕЗН_ПЛ_ТРЕБ' then 3
              when DOCUMENT_TYPE_CD = 'КАСС_ОБЪЯВ_ВЗНОС' then 4
              when DOCUMENT_TYPE_CD = 'БЕЗН_МЕМО_ОРД' then 5
              when DOCUMENT_TYPE_CD = 'БЕЗН_ПЛ_ОР' then 6
              when DOCUMENT_TYPE_CD = 'КАСС_ЧЕК_ВЫДАЧА' then 7
              when DOCUMENT_TYPE_CD = 'БЕЗН_ИНКАСС_ПОР' then 8
              else 0   
        end) as type_code
       ,trtype_2(DOCUMENT_DESC, DOCUMENT_TYPE_CD, DBCR_CD) as subtype_code
       
       ,round( to_number(DOCUMENT_AMT)*(case when ACCOUNT_CCY_CD = '810' then 1
                                             when ACCOUNT_CCY_CD = '840' then 65
                                             when ACCOUNT_CCY_CD = '978' then 72
                                             ELSE 1 END) ) as val
       ,CUSTOMER_2_INN as cp_inn
       from (select t1.CUSTOMER_INN, 
                    t1.BRANCH_CD, 
                    t1.DOCUMENT_DT, 
                    t1.DBCR_CD,
                    t1.DOCUMENT_TYPE_CD, 
                    t1.DOCUMENT_DESC, 
                    t1.ACCOUNT_CCY_CD, 
                    t1.DOCUMENT_AMT, 
                    t1.CUSTOMER_2_INN 
              from kozhina_es.eks_transact_20161223_dan t1);

/*********************************/
-- Шаг 2. Агрегация на уровне месяца
/*********************************/
create table eks_to_promdm_dan1_2_part1 as
select  
       t4.*
       ,round((max(sal - sal_avg_1_6) over 
       (partition by t4.inn order by t4.cutoff desc range between current row and interval '5' month following)) /  	 	      	  (sal_avg_1_6 + 0.1), 2)  as sal_6m_dev_max
       ,round((min(sal - sal_avg_1_6) over (partition by t4.inn order by t4.cutoff desc range between current row and interval 	  '5' month following)) / (sal_avg_1_6 + 0.1), 2)  as sal_6m_dev_min
       ,sum(case when sal - sal_avg_1_6 < 0 then 1 else 0 end) over (partition by t4.inn order by t4.cutoff desc range between 	  current row and interval '5' month following)  as sal_6m_dev_neg_cnt
       ,round((max(sal - sal_avg_1_12) over (partition by t4.inn order by t4.cutoff desc range between current row and interval 	'11' month following)) / (sal_avg_1_12 + 0.1), 2)  as sal_12m_dev_max
       ,round((min(sal - sal_avg_1_12) over (partition by t4.inn order by t4.cutoff desc range between current row and interval 	'11' month following)) / (sal_avg_1_12 + 0.1), 2)  as sal_12m_dev_min
       ,sum(case when sal - sal_avg_1_12 < 0 then 1 else 0 end) over (partition by t4.inn order by t4.cutoff desc range between 	current row and interval '11' month following)  as sal_12m_dev_neg_cnt
  from (select inn
		   ,row_number() over (partition by inn order by cutoff desc) as n
	    	   ,count(*) over (partition by inn) as cnt
		   ,cutoff ,mth 
              ,max(cutoff) over (partition by inn) as cutoff_max
              ,min(cutoff) over (partition by inn) as cutoff_min
              --,months_between(cutoff, min(cutoff) over (partition by inn)) as dur 
              -- текущий месяц
              ,sal, 
              sal_90, sal_95, 
              ,gap_sal_90, gap_sal_nonc_90
              -- Режим налообложения
              ,max(case when tax_profit > 0 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between unbounded preceding and unbounded following) as tax_profit_fl
              ,max(case when tax_simp > 0 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between unbounded preceding and unbounded following) as tax_simp_fl
              ,max(case when tax_unit > 0 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between unbounded preceding and unbounded following) as tax_unit_fl
               -- Выручка
              ,round(avg(case when mth <> 1 then to_c else null end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) as toc_avg_1_6
              ,round(avg(case when mth <> 1 then to_c - to_d else null end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) as delta_avg_1_6
              ,round(avg(case when mth <> 1 then sal else null end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) as sal_avg_1_6
              ,round(sum(sal) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) as sal_sum_1_6
              ,round( (stddev_samp(case when mth <> 1 then sal else null end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) / 
               ( avg(case when mth <> 1 then sal else null end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) + 0.1) ,2) as sal_stddev_1_6
              ,round(avg(case when mth <> 1 then to_c else null end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following)) as toc_avg_1_12
              ,round(avg(case when mth <> 1 then to_c - to_d else null end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following)) as delta_avg_1_12
              ,round(avg(case when mth <> 1 then sal else null end) 
               over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following)) as sal_avg_7_12
              ,round(sum(sal) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following)) as sal_sum_1_12       
              ,round(avg(case when mth <> 1 then sal else null end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following)) as sal_avg_1_12
              ,round( (stddev_samp(case when mth <> 1 then sal else null end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following)) / 
               ( avg(case when mth <> 1 then sal else null end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following) + 0.1) ,2) as sal_stddev_1_12
              ,round( avg(case when mth <> 1 then sal_90 else null end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) ,2) as sal_90_1_6
              ,round( avg(case when mth <> 1 then sal_95 else null end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) ,2) as sal_95_1_6
              ,round( avg(case when mth <> 1 then gap_sal_90 else null end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) as gap_sal_90_1_6
              ,round( avg(case when mth <> 1 then gap_sal_nonc_90 else null end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) as gap_sal_nonc_90_1_6
              ,round( avg(case when mth <> 1 then gap_sal_rec_90 else null end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) as gap_sal_rec_90_1_6
              ,round((sum(cashin) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) / 
               (sum(sal) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) + 0.1) , 2) as cashin_r_1_6
              ,round((sum(acq) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) / 
               (sum(sal) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) + 0.1) , 2) as acq_r_1_6
              ,round((sum(merch) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) / 
               (sum(sal) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) + 0.1) , 2) as merch_r_1_6
              ,round((sum(mbk) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) / 
               (sum(sal) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) + 0.1) , 2) as mbk_r_1_6
              ,round((sum(inc_rent) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) / 
               (sum(sal) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) + 0.1) , 2) as inc_rent_r_1_6
              ,round((sum(inc_trans) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) / 
               (sum(sal) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) + 0.1) , 2) as inc_trans_r_1_6
              ,round( (avg(case when mth <> 1 then sal else null end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '2' month following)) / 
               (avg(case when mth <> 1 then sal else null end) 
               over (partition by inn order by cutoff desc range 
               between interval '3' month following and interval '5' month following) + 0.1) , 2 ) as sal_tr_13_46
              ,round( (avg(case when mth <> 1 then sal else null end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '2' month following)) / 
               (avg(case when mth <> 1 then sal else null end) 
               over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following) + 0.1) , 2 ) as sal_tr_13_712
              ,round( (avg(case when mth <> 1 then sal else null end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) / 
               (avg(case when mth <> 1 then sal else null end) 
               over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following) + 0.1) , 2 ) as sal_tr_16_712
              ,round( (avg(case when mth <> 1 then sal else null end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) / 
               (avg(case when mth <> 1 then sal else null end) 
               over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following) + 0.1) , 2 ) as sal_tr_16_1324
              ,round( (avg(case when mth <> 1 then sal else null end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following)) / 
               (avg(case when mth <> 1 then sal else null end) 
               over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following) + 0.1) , 2 ) as sal_tr_112_1324
              ,round( (stddev_samp(case when mth <> 1 then sal else null end) 
               over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following)) / 
               (avg(case when mth <> 1 then sal else null end) 
               over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following) + 0.1), 2) as sal_stddev_7_12
              ,round( (stddev_samp(case when mth <> 1 then sal else null end) 
               over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following)) / 
               (avg(case when mth <> 1 then sal else null end) over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following) + 0.1) , 2) as sal_stddev_13_24
               -- new for ip
              ,round(min(sal) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following)) as sal_min_1_12   
               -- Зарплата
              ,sum(wage) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) as wage_sum_1_6
              ,round(avg(wage) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) as wage_avg_1_6
              ,sum(case when wage > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) as wage_cnt_1_6
              ,round( (stddev_samp(wage) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) / 
               (avg(wage) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) + 0.1) , 2 ) as wage_stddev_1_6
              ,round( (stddev_samp(wage) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following)) / 
               (avg(wage) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following) + 0.1) , 2 ) as wage_stddev_1_12
              ,round( (avg(wage) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) / 
               (avg(wage) over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following) + 0.1) , 2 ) as wage_tr_16_712
              ,round( (avg(wage) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) / 
               (avg(wage) over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following) + 0.1) , 2 ) as wage_tr_16_1324
              ,round( (avg(wage) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following)) / 
               (avg(wage) over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following) + 0.1) , 2 ) as wage_tr_112_1324
               -- Кредитная история
              ,sum(case when lo_del_repay > 0 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) as del_repay_cnt_1_6
              ,sum(case when lo_del_repay > 5 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) as del5_repay_cnt_1_6
              ,sum(lo_del_repay) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) as del_repay_sum_1_6
              ,sum(case when lo_del_repay > 0 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following) as del_repay_cnt_7_12
              ,sum(case when lo_del_repay > 5 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following) as del5_repay_cnt_7_12
              ,sum(lo_del_repay) over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following) as del_repay_sum_7_12
              ,sum(case when lo_del_repay > 0 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following) as del_repay_cnt_1_12
              ,sum(case when lo_del_repay > 5 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following) as del5_repay_cnt_1_12
              ,sum(lo_del_repay) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following) as del_repay_sum_1_12       
              ,sum(case when lo_del_repay > 0 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following) as del_repay_cnt_13_24
              ,sum(case when lo_del_repay > 5 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following) as del5_repay_cnt_13_24
              ,sum(lo_del_repay) over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following) as del_repay_sum_13_24
              -- Долговая нагрузка
              ,sum(lo_repay) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) as repay_sum_1_6
              ,round(avg(lo_repay) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) as repay_avg_1_6
              ,sum(case when lo_repay > 0 then 1 else 0 end) over 
               (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) as repay_cnt_1_6 
              ,round( (stddev_samp(lo_repay) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) / 
               (avg(lo_repay) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) + 0.1) ,2 ) as repay_stddev_1_6
              ,sum(lo_repay) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following) as repay_sum_1_12
              ,round(avg(lo_repay) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following)) as repay_avg_1_12
              ,sum(case when lo_repay > 0 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following) as repay_cnt_1_12        
              ,round( (stddev_samp(lo_repay) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following)) / (avg(lo_repay) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following) + 0.1) ,2 ) as repay_stddev_1_12
               -- Полученные кредиты
              ,sum(lo_receiv) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) as receiv_sum_1_6
              ,round(avg(lo_receiv) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) as receiv_avg_1_6
              ,sum(case when lo_receiv > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) as receiv_cnt_1_6       
              ,sum(lo_receiv) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following) as receiv_sum_1_12
              ,round(avg(lo_receiv) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following)) as receiv_avg_1_12
              ,sum(case when lo_receiv > 0 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following) as receiv_cnt_1_12  
              -- Полученные - погашенные кредиты
              ,sum(lo_repay-lo_receiv) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) as lodelta_sum_1_6
              ,round(avg(lo_repay-lo_receiv) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) as lodelta_avg_1_6
              ,sum(lo_repay-lo_receiv) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following) as lodelta_sum_1_12
              ,round(avg(lo_repay-lo_receiv) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following)) as lodelta_avg_1_12
              ,round( (avg(lo_repay) over (partition by inn order by cutoff desc range 
               between current row and interval '2' month following)) / 
               (avg(lo_repay) over (partition by inn order by cutoff desc range 
               between interval '3' month following and interval '5' month following) + 0.1) , 2 ) as repay_tr_13_46
              ,round( (avg(lo_repay) over (partition by inn order by cutoff desc range 
               between current row and interval '2' month following)) / 
               (avg(lo_repay) over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following) + 0.1) , 2 ) as repay_tr_13_712
              ,round( (avg(lo_repay) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) / 
               (avg(lo_repay) over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following) + 0.1) , 2 ) as repay_tr_16_712
              ,round( (avg(lo_repay) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) / 
               (avg(lo_repay) over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following) + 0.1) , 2 ) as repay_tr_16_1324
              ,round( (avg(lo_repay) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following)) / 
               (avg(lo_repay) over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following) + 0.1) , 2 ) as repay_tr_112_1324
               -- Исполнительное
              ,sum(case when ispoln > 0 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) as ispoln_cnt_1_6
              ,sum(case when ispoln > 5 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) as ispoln5_cnt_1_6
              ,sum(ispoln) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) as ispoln_sum_1_6
              ,sum(case when ispoln > 0 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following) as ispoln_cnt_7_12
              ,sum(case when ispoln > 5 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following) as ispoln5_cnt_7_12
              ,sum(ispoln) over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following) as ispoln_sum_7_12
              ,sum(case when ispoln > 0 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following) as ispoln_cnt_1_12
              ,sum(case when ispoln > 5 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following) as ispoln5_cnt_1_12
              ,sum(ispoln) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following) as ispoln_sum_1_12
              ,sum(case when ispoln > 0 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following) as ispoln_cnt_13_24
              ,sum(case when ispoln > 5 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following) as ispoln5_cnt_13_24
              ,sum(ispoln) over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following) as ispoln_sum_13_24    
               -- Штрафы по налогам
              ,sum(case when pen_tax > 0 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) as pentax_cnt_1_6
              ,sum(case when pen_tax > 5 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) as pentax5_cnt_1_6
              ,sum(pen_tax) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) as pentax_sum_1_6
              ,sum(case when pen_tax > 0 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following) as pentax_cnt_7_12
              ,sum(case when pen_tax > 5 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following) as pentax5_cnt_7_12
              ,sum(pen_tax) over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following) as pentax_sum_7_12
              ,sum(case when pen_tax > 0 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following) as pentax_cnt_1_12
              ,sum(case when pen_tax > 5 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following) as pentax5_cnt_1_12
              ,sum(pen_tax) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following) as pentax_sum_1_12       
              ,sum(case when pen_tax > 0 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following) as pentax_cnt_13_24
              ,sum(case when pen_tax > 5 then 1 else 0 end)  
               over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following) as pentax5_cnt_13_24
              ,sum(pen_tax) over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following) as pentax_sum_13_24 
               -- Штрафы прочие
              ,sum(case when pen_oth > 0 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) as penoth_cnt_1_6
              ,sum(case when pen_oth > 5 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) as penoth5_cnt_1_6
              ,sum(pen_oth) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) as penoth_sum_1_6
              ,sum(case when pen_oth > 0 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following) as penoth_cnt_7_12
              ,sum(case when pen_oth > 5 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following) as penoth5_cnt_7_12
              ,sum(pen_oth) over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following) as penoth_sum_7_12
              ,sum(case when pen_oth > 0 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following) as penoth_cnt_1_12
              ,sum(case when pen_oth > 5 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following) as penoth5_cnt_1_12
              ,sum(pen_oth) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following) as penoth_sum_1_12      
              ,sum(case when pen_oth > 0 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following) as penoth_cnt_13_24
              ,sum(case when pen_oth > 5 then 1 else 0 end) 
               over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following) as penoth5_cnt_13_24
              ,sum(pen_oth) over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following) as penoth_sum_13_24
              -- НДС
              ,sum(case when tax_nds > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) as nds_cnt_1_6
              ,sum(tax_nds) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) as nds_sum_1_6
              ,sum(case when tax_nds > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following) as nds_cnt_7_12
              ,sum(tax_nds) over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following) as nds_sum_7_12
              ,sum(case when tax_nds > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following) as nds_cnt_1_12
              ,sum(tax_nds) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following) as nds_sum_1_12       
              ,sum(case when tax_nds > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following) as nds_cnt_13_24
              ,sum(tax_nds) over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following) as nds_sum_13_24
              -- Налог на прибыль
              ,sum(case when tax_profit > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) as taxprofit_cnt_1_6
              ,round(avg(tax_profit) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) as taxprofit_avg_1_6
              ,sum(case when tax_profit > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following) as taxprofit_cnt_7_12
              ,round(avg(tax_profit) over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following)) as taxprofit_avg_7_12
              ,sum(case when tax_profit > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following) as taxprofit_cnt_1_12
              ,round(avg(tax_profit) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following)) as taxprofit_avg_1_12       
              ,sum(case when tax_profit > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following) as taxprofit_cnt_13_24
              ,round(avg(tax_profit) over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following)) as taxprofit_avg_13_24
               -- Упрощенный налог
              ,sum(case when tax_simp > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) as taxsimp_cnt_1_6
              ,round(avg(tax_simp) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) as taxsimp_avg_1_6
              ,sum(case when tax_simp > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following) as taxsimp_cnt_7_12
              ,round(avg(tax_simp) over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following)) as taxsimp_avg_7_12
              ,sum(case when tax_simp > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following) as taxsimp_cnt_1_12
              ,round(avg(tax_simp) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following)) as taxsimp_avg_1_12       
              ,sum(case when tax_simp > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following) as taxsimp_cnt_13_24
              ,round(avg(tax_simp) over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following)) as taxsimp_avg_13_24
              -- Единый налог
              ,sum(case when tax_unit > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) as taxunit_cnt_1_6
              ,round(avg(tax_unit) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) as taxunit_avg_1_6
              ,sum(case when tax_unit > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following) as taxunit_cnt_7_12
              ,round(avg(tax_unit) over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following)) as taxunit_avg_7_12
              ,sum(case when tax_unit > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following) as taxunit_cnt_1_12
              ,round(avg(tax_unit) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following)) as taxunit_avg_1_12       
              ,sum(case when tax_unit > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following) as taxunit_cnt_13_24
              ,round(avg(tax_unit) over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following)) as taxunit_avg_13_24
               -- Налоги на доход
              ,sum(case when tax_unit > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) as taxinc_cnt_1_6
              ,round(avg(tax_unit) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) as taxinc_avg_1_6
              ,sum(case when tax_unit > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following) as taxinc_cnt_7_12
              ,round(avg(tax_unit) over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following)) as taxinc_avg_7_12
              ,sum(case when tax_unit > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following) as taxinc_cnt_1_12
              ,round(avg(tax_unit) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following)) as taxinc_avg_1_12       
              ,sum(case when tax_unit > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following) as taxinc_cnt_13_24
              ,round(avg(tax_unit) over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following)) as taxinc_avg_13_24
               -- Страховые взносы
              ,sum(case when tax_oth > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) as tax_cnt_1_6
              ,round(avg(tax_oth) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) as tax_avg_1_6
              ,sum(case when tax_oth > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following) as tax_cnt_7_12
              ,round(avg(tax_oth) over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following)) as tax_avg_7_12
              ,sum(case when tax_oth > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following) as tax_cnt_1_12
              ,round(avg(tax_oth) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following)) as tax_avg_1_12       
              ,sum(case when tax_oth > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following) as tax_cnt_13_24
              ,round(avg(tax_oth) over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following)) as tax_avg_13_24
               -- Суммарные налоги
              ,sum(case when tax_sum > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) as taxsum_cnt_1_6
              ,round(avg(tax_sum) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) as taxsum_avg_1_6
              ,sum(case when tax_sum > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following) as taxsum_cnt_7_12
              ,round(avg(tax_sum) over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following)) as taxsum_avg_7_12
              ,sum(case when tax_sum > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following) as taxsum_cnt_1_12
              ,round(avg(tax_sum) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following)) as taxsum_avg_1_12       
              ,sum(case when tax_sum > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following) as taxsum_cnt_13_24
              ,round(avg(tax_sum) over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following)) as taxsum_avg_13_24
               -- Профиль расходов
              ,round(avg(selfcost+transport+rental) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) as selfcost_avg_1_6
              ,round(avg(transport) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) as transport_avg_1_6
              ,round(avg(rental) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) as rental_avg_1_6
              ,round( (sum(sal - (selfcost+transport+rental)) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) / 
               (sum(sal) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following) + 0.1), 2) as profit_r_1_6       
              ,round(avg(selfcost+transport+rental) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following)) as selfcost_avg_1_12
              ,round(avg(transport) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following)) as transport_avg_1_12
              ,round(avg(rental) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following)) as rental_avg_1_12
              ,round( (sum(sal - (selfcost+transport+rental)) 
               over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following)) / 
               (sum(sal) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following) + 0.1), 2) as profit_r_1_12  
               -- Выведено из бизнеса
              ,round(avg(bus_out) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) as busout_avg_1_6
              ,round(avg(bus_out) over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following)) as busout_avg_7_12
              ,round(avg(bus_out) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following)) as busout_avg_1_12       
              ,round(avg(bus_out) over 
               (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following)) as busout_avg_13_24
               -- Выдано кредитов
              ,round(avg(lo_adv) over (partition by inn order by cutoff desc range 
               between current row and interval '5' month following)) as loadv_avg_1_6
              ,round(avg(lo_adv) over (partition by inn order by cutoff desc range 
               between interval '6' month following and interval '11' month following)) as loadv_avg_7_12
              ,round(avg(lo_adv) over (partition by inn order by cutoff desc range 
               between current row and interval '11' month following)) as loadv_avg_1_12
              ,round(avg(lo_adv) over (partition by inn order by cutoff desc range 
               between interval '12' month following and interval '23' month following)) as loadv_avg_13_24
          from (select inn, cutoff, extract(month from cutoff) as mth
                    -- Для анализа корректности расчетов
                    --        ,sum(in1) as in1
                    --        ,sum(in2) as in2
                    --        ,sum(in4) as in4 
                    --        ,sum(in5_inc) as in5_inc               
                    ,sum(sal) as sal
                    --  Доля поступлений на 90+ процентиль распределения суммы поступлений 
                    ,round( sum(case when sal_dist > 0.9 then sal else 0 end) / (sum(sal) + 0.1) , 2) as sal_90   
                    --  Доля поступлений на 95+ процентиль распределения суммы поступлений 
                    ,round( sum(case when sal_dist > 0.95 then sal else 0 end) / (sum(sal) + 0.1) , 2) as sal_95   
                    -- 90 процентиль распределения интервалов между последовательными поступлениями
                    ,min(case when gap_sal_dist > 0.9 then gap_sal else 35 end) as gap_sal_90       
                    ,min(case when gap_sal_nonc_dist > 0.9 then gap_sal else 35 end) as gap_sal_nonc_90          
                    ,min(case when gap_sal_rec_dist > 0.9 then gap_sal else 35 end) as gap_sal_rec_90 
                    -- profile
                    ,sum(cash_in) as cashin
                    ,sum(acq) as acq
                    ,sum(merch) as merch
                    ,sum(mbk) as mbk
                    ,sum(inc_rent) as inc_rent
                    ,sum(inc_trans) as inc_trans
                    ,sum(wage) as wage
                    ,sum(selfcost) as selfcost
                    ,sum(transport) as transport
                    ,sum(rental) as rental
                    ,sum(ispoln) as ispoln
                    ,sum(pen_tax) as pen_tax
                    ,sum(pen_oth) as pen_oth                                        
                    ,sum(tax_nds) as tax_nds
                    ,sum(tax_oth) as tax_oth 
                    ,sum(tax_profit) as tax_profit  
                    ,sum(tax_simp) as tax_simp 
                    ,sum(tax_unit) as tax_unit
                    ,sum(tax_oth + tax_unit + tax_simp + tax_profit) as tax_sum
                    ,sum(tax_unit + tax_simp + tax_profit) as tax_inc
                    ,sum(transin) as transin
                    ,sum(transout) as transout 
                    ,sum(lo_receiv) as lo_receiv
                    ,sum(lo_repay) as lo_repay
                    ,sum(lo_del_repay) as lo_del_repay
                    ,sum(lo_early_repay) as lo_early_repay
                    ,sum(lo_adv) as lo_adv
                    ,sum(bus_out) as bus_out
                    ,round(sum(cashout)/ (sum(sal) +0.1),2) as cashout_r        
                    ,round(sum(lo_adv_cash)/(sum(sal) +0.1),2) as lo_adv_cash_r
                    ,round(sum(onacc_cash)/(sum(sal) +0.1),2) as onacc_cash_r
                    ,round(sum(onacc)/(sum(sal) +0.1),2) as onacc_r
                    ,round(sum(onacc_back)/(sum(sal) +0.1),2) as onacc_back_r
                    ,sum(to_d) as to_d
                    ,sum(to_c) as to_c
                    from (select t2.*
                                 ,round(cume_dist() 
                                  over 
                                 (partition by inn, cutoff order by gap_sal) , 2) as gap_sal_dist
                                 ,round(cume_dist() 
                                  over 
                                 (partition by inn, cutoff order by gap_sal_nonc) , 2) as gap_sal_nonc_dist
                                 ,round( cume_dist() 
                                  over (partition by inn, cutoff order by gap_sal_rec) , 2) as gap_sal_rec_dist
                           from (select t.*
                                       ,round( cume_dist() over (partition by inn, cutoff order by sal) , 2) as sal_dist
                                       ,round( cume_dist() over 
                                       (partition by inn, cutoff order by sal_nonc) , 2) as sal_nonc_dist
                                       ,(dt - last_value(case when sal > 0 then dt else null end ignore nulls) 
                                        over (partition by inn order by dt range 
                                        between unbounded preceding and 1 preceding)) as gap_sal
                                       ,(dt - last_value(case when sal_nonc > 0 then dt else null end ignore nulls) 
                                        over (partition by inn order by dt range 
                                        between unbounded preceding and 1 preceding)) as gap_sal_nonc
                                       ,(dt - last_value(case when sal_rec > 0  then dt else null end ignore nulls) 
                                        over (partition by inn order by dt range 
                                        between unbounded preceding and 1 preceding)) as gap_sal_rec
                                        -- стандартное отклонение выручки
                                   from (select inn, 
                                                to_date(dt, 'dd.mm.yyyy') as dt
                                                max(trunc(to_date(dt, 'dd.mm.yyyy'), 'month')) as cutoff, 
                                                -- Суммарные операции по дебиту
                                                ,round(sum(case when debit = 1 then val else 0 end)/1000) as to_d
                                                ,round(sum(case when debit = 0 then val else 0 end)/1000) as to_c   
                                                -- Выручка
                                                ,round(sum(case when (   type_code in (1, 2, 4) and debit = 0) 
                                                                      or SUBTYPE_CODE in ('5_0_7') 
                                                                then val else 0 end)/1000) as sal0
                                                ,round(sum(case when type_code in (1) and debit = 0 
                                                                then val else 0 end)/1000) as in1                                 
                                                ,round(sum(case when type_code in (2) and debit = 0 
                                                                then val else 0 end)/1000) as in2
                                                ,round(sum(case when type_code in (4) and debit = 0 
                                                                then val else 0 end)/1000) as in4
                                                ,round(sum(case when SUBTYPE_CODE in ('5_0_7') 
                                                                then val else 0 end)/1000) as in5_inc                                                                                                                 
                                                ,round(sum(case when SUBTYPE_CODE in ('1_0_2', '1_0_4', '1_0_5', '1_0_6', 
                                                                                     '1_0_7', '1_0_8', '1_0_9', '1_0_10',
                                                                                     '1_0_12', '4_0_1','2_0_1','2_0_2',
                                                                                     '2_0_3','5_0_7') 
                                                               then val else 0 end)/1000) as sal
                                                ,round(sum(case when SUBTYPE_CODE in ('1_0_7','1_0_8','1_0_9', '1_0_10',
                                                                                      '1_0_12') 
                                                                then val else 0 end)/1000) as sal_rec 
                                                ,round(sum(case when SUBTYPE_CODE in ('1_0_7','1_0_8','1_0_9', '1_0_10',
                                                                                     '1_0_12','4_0_1', '2_0_1','1_0_5',
                                                                                     '2_0_2') 
                                                               then val else 0 end)/1000) as sal_nonc  
                                                -- Инкассация
                                                ,round(sum(case when SUBTYPE_CODE in ('1_0_2','4_0_1','5_0_7') 
                                                                then val else 0 end)/1000) as cash_in
                                                -- Эквайринг
                                                ,round(sum(case when SUBTYPE_CODE in ('1_0_4','2_0_1') 
                                                                then val else 0 end)/1000) as acq                                                       
                                                -- Мерчант
                                                ,round(sum(case when SUBTYPE_CODE in ('1_0_5','2_0_2') 
                                                                then val else 0 end)/1000) as merch
                                                -- МБК
                                                ,round(sum(case when SUBTYPE_CODE in ('1_0_6','2_0_3') 
                                                                then val else 0 end)/1000) as mbk
                                                -- Аренда - доходы
                                                ,round(sum(case when SUBTYPE_CODE in ('1_0_7') 
                                                                then val else 0 end)/1000) as inc_rent
                                                -- Транспорт - доходы
                                                ,round(sum(case when SUBTYPE_CODE in ('1_0_8') 
                                                                then val else 0 end)/1000) as inc_trans
                                                -- Себестоимость
                                                ,round(sum(case when SUBTYPE_CODE in ('1_1_12','1_1_13','1_1_15',
                                                                                      '1_1_16','3_1_5','6_1_6') 
                                                               then val else 0 end)/1000) as selfcost
                                                -- Транспорт - расходы
                                                ,round(sum(case when SUBTYPE_CODE in ('1_1_12') 
                                                                then val else 0 end)/1000) as transport
                                                -- Аренда - расходы
                                                ,round(sum(case when SUBTYPE_CODE in ('1_1_13') 
                                                                then val else 0 end)/1000) as rental
                                                -- Исполнительное
                                                ,round(sum(case when SUBTYPE_CODE in ('1_1_1','6_1_2','8_1_3') 
                                                                then val else 0 end)/1000) as ispoln
                                                -- Штрафы по налогам
                                                ,round(sum(case when SUBTYPE_CODE in ('1_1_7','6_1_2','6_1_3','8_1_2')                       							 		    	          then val else 0 end)/1000) as pen_tax
                                                -- Штрафы прочие
                                                ,round(sum(case when SUBTYPE_CODE in ('1_1_8') 
                                                                then val else 0 end)/1000) as pen_oth
                                               -- НДС
                                               ,round(sum(case when SUBTYPE_CODE in ('1_1_17','6_1_11') 
                                                               then val else 0 end)/1000) as tax_nds
                                               -- Налог на прибыль и НДФЛ
                                               ,round(sum(case when SUBTYPE_CODE in ('1_1_6','1_1_20','6_1_7', '6_1_12') 
                                                               then val else 0 end)/1000) as tax_profit
                                               -- Упрощенный
                                               ,round(sum(case when SUBTYPE_CODE in ('1_1_21','6_1_13') 
                                                               then val else 0 end)/1000) as tax_simp
                                               -- Единый
                                               ,round(sum(case when SUBTYPE_CODE in ('1_1_22','6_1_14') 
                                                               then val else 0 end)/1000) as tax_unit
                                               -- Прочие налоги (в основном страховые взносы)
                                               ,round(sum(case when SUBTYPE_CODE in ('1_1_6', '1_1_9', '6_1_7', '6_1_8') 
                                                               then val else 0 end)/1000) as tax_oth
                                               -- Трансфер
                                               ,round(sum(case when SUBTYPE_CODE in ('1_1_19') 
                                                               then val else 0 end)/1000) as transout
                                               ,round(sum(case when SUBTYPE_CODE in ('1_0_11', '5_0_8') 
                                                               then val else 0 end)/1000) as transin
                                               -- Выдано наличными
                                               ,round(sum(case when type_code = 7 and debit = 1 
                                                               then val else 0 end)/1000) as cashout
                                               -- Выдано кредитов наличными
                                               ,round(sum(case when SUBTYPE_CODE in ('7_1_1') 
                                                               then val else 0 end)/1000) as lo_adv_cash
                                               -- Выдано наличными на хоз нужды/под отчет
                                               ,round(sum(case when SUBTYPE_CODE in ('7_1_3','7_1_4') 
                                                               then val else 0 end)/1000) as onacc_cash
                                               -- Выдано под отчет
                                               ,round(sum(case when SUBTYPE_CODE in ('1_1_10','7_1_3','7_1_4') 
                                                               then val else 0 end)/1000) as onacc
                                               -- Получено из под отчет
                                               ,round(sum(case when SUBTYPE_CODE in ('1_0_3') 
                                                               then val else 0 end)/1000) as onacc_back
                                               -- Выдано кредитов
                                               ,round(sum(case when SUBTYPE_CODE in ('1_1_20','7_1_1') 
                                                               then val else 0 end)/1000) as lo_adv
                                               -- Выведено из бизнеса
                                               ,round(sum(case when SUBTYPE_CODE in ('1_1_2') 
                                                               then val else 0 end)/1000) as bus_out
                                               -- Заработная плата
                                               ,round(sum(case when SUBTYPE_CODE in ('1_1_11','3_1_3','6_1_9') 
                                                               then val else 0 end)/1000) as wage                                                      
                                               -- Получено кредитов
                                               ,round(sum(case when SUBTYPE_CODE in ('1_0_1', '5_0_5', '5_0_6') 
                                                               then val else 0 end)/1000) as lo_receiv
                                               -- Погашено кредитов
                                               ,round(sum(case when SUBTYPE_CODE in ('1_1_3','1_1_4','1_1_5', '3_1_1',
                                                                                     '3_1_2','5_1_1', '5_1_2','6_1_1',
                                                                                     '6_1_4', '8_1_1') 
                                                               then val else 0 end)/1000) as lo_repay 
                                               -- Погашено просроченной задолженности
                                               ,round(sum(case when SUBTYPE_CODE in ('1_1_3','3_1_1','5_1_1', '6_1_1','8_1_1') 
                                                               then val else 0 end)/1000) as lo_del_repay 
                                               -- Погашено досрочно
                                               ,round(sum(case when SUBTYPE_CODE in ('1_1_4') 
                                                               then val else 0 end)/1000) as lo_early_repay 
         /* Сумма платежей по исполнительному производству, 
            не свяхзанному с налогами (ип_1), за последние 3/6/7-12 мес
            Сумма платежей по ип_1 за последние 12 мес / Сумма платежей по ип_1 за последние 13-24 мес
            Сумма штрафов по налогам и сборам (ип_2), за последние 3/6/7-12 мес */
                                         from kozhina_es.eks_to_calc_161223_dan 
                                        group by inn, dt
                                       ) t 
                                ) t2
                         ) t3     
      group by inn, cutoff)
      ) t4;
                                                                                                                
                                                                                                                

create table eks_to_dan1_cust1_part1_2610 as 
select inn, 
       substr(inn, 1, 2) as reg,
       cp_inn, 
       substr(cp_inn,1,2) as cp_reg,
       ---!!!!!!!!!! Подумать  
       round(sal / (sum(sal) over (partition by inn) + 0.1) * 100, 2) as sal_r 
 from (select inn, 
              cp_inn, 
              sum(val) as sal
         from kozhina_es.eks_to_calc_161223_dan 
        where SUBTYPE_CODE in ('1_0_7','1_0_8','1_0_9','1_0_10','1_0_12')
        group by inn, cp_inn);
                                 
-----------------------------------------------------------------------------
/******************/
/* ALL SUPPLIERS */                                                
/******************/
create table eks_to_promdm_dan1_sup1_part1 as
select inn, 
       substr(inn, 1, 2) as reg,
       cp_inn, 
       substr(cp_inn,1,2) as cp_reg,
       round(sal / (sum(sal) over (partition by inn) + 1) * 100, 2) as sal_r 
  from (select inn, 
               cp_inn, 
               sum(val) as sal
          from from kozhina_es.eks_to_calc_161223_dan 
         where SUBTYPE_CODE in ('1_1_12','1_1_13','1_1_15','1_1_16','3_1_5','6_1_6')
         group by inn, cp_inn);
/*******************************************************************************************/

/*********************************/
-- Шаг 3. Витрина покупателей
/*********************************/
----create table eks_to_promdm_dan1_cust2_part1 as / ***Изменено!!!*** Иное название таблицы/
create table eks_to_dan1_cust2_part1_2610 as 
select inn, min(dur) as dur, cutoff,
       sum(case when val > 0 then 1 else 0 end) as cust_n,
       min(round(cnt_dist/6)) as cust_n_6m,
       round(sum(case when val > 0 and cnt_6m > 0 then 1 else 0 end) / 
             (sum(case when val > 0 then 1 else 0 end) + 0.01), 2) as cust_rot_6m_cnt
       ,round(sum(case when val > 0 and cnt_6m > 0 then val else 0 end) / (sum(val) + 0.1), 2) as cust_rot_6m_sum
       ,round(sum(case when n = 1 then val_6m else 0 end)/ (sum(val_6m) + 0.1), 2) as cust_1_big
       ,round(sum(case when n <= 3 then val_6m else 0 end)/ (sum(val_6m) + 0.1), 2) as cust_3_big
       ,round(sum(case when n <= 5 then val_6m else 0 end)/ (sum(val_6m) + 0.1), 2) as cust_5_big
       ,min(case when n = 1 then cp_inn end) as cust_1
       ,round (sum(case when n = 1 then val_6m else 0 end)/ (sum(val_6m) + 0.1), 2) as cust_1_r
       ,min(case when n = 2 then cp_inn end) as cust_2
       ,round(sum(case when n = 2 then val_6m else 0 end)/ (sum(val_6m) + 0.1), 2) as cust_2_r
       ,min(case when n = 3 then cp_inn end) as cust_3
       ,round(sum(case when n = 3 then val_6m else 0 end)/ (sum(val_6m) + 0.1), 2) as cust_3_r
       ,min(case when n = 4 then cp_inn end) as cust_4
       ,round(sum(case when n = 4 then val_6m else 0 end)/ (sum(val_6m) + 0.1), 2) as cust_4_r
       ,min(case when n = 5 then cp_inn end) as cust_5
       ,round(sum(case when n = 5 then val_6m else 0 end)/ (sum(val_6m) + 0.1), 2) as cust_5_r 
  from (select inn, dur, cp_inn, cutoff, cnt_dist, cnt_6m, val, val_6m, 
               cume_dist() over (partition by inn, cutoff order by val_6m) as val_6m_cum,
               row_number() over (partition by inn, cutoff order by val_6m desc) as n 
          from (select inn, cp_inn, cutoff, val,
                 sum(case when val > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range between current row and interval '5' month following) as cnt_dist,
                 sum(val) over (partition by inn, cp_inn order by cutoff desc range between current row and interval '5' month following) as val_6m,
                 sum(case when val > 0 then 1 else 0 end) over (partition by inn, cp_inn order by cutoff desc range between interval '1' month following and interval '6' month following) as cnt_6m,
                 months_between(cutoff, min(cutoff) over (partition by inn)) as dur
                  from (select t3.inn, 
                               t3.cp_inn, 
                               t3.cutoff, 
                               (case when t4.val is null then 0 else t4.val end) as val 
                          from (select t2.inn, t2.cp_inn, t1.cutoff
                                  from eks_to_dan1_cust1_part1_2610 t2,
                                       (select inn, 
                                               trunc(to_date(dt,'dd.mm.yyyy'), 'month') as cutoff 
                                                         from kozhina_es.eks_to_calc_161223_dan 
                                         where SUBTYPE_CODE in ('1_0_7','1_0_8','1_0_9','1_0_10','1_0_12')) 
                                         group by inn, cutoff) t1
                                 where t2.inn = t1.inn 
                               ) t3 
                               left join 
                               /* Эту таблицу нужно сохранить отдельно */
                               (select inn, 
                                       cp_inn, 
                                       trunc(to_date(dt,'dd.mm.yyyy'), 'month') as cutoff, 
                                       sum(val) as val 
                                  from kozhina_es.eks_to_calc_161223_dan 
                                 where SUBTYPE_CODE in ('1_0_7','1_0_8','1_0_9','1_0_10','1_0_12'))
                                 group by inn, cp_inn, cutoff
                               ) t4
                               on t3.inn = t4.inn 
                               and t3.cp_inn = t4.cp_inn 
                               and t3.cutoff = t4.cutoff
                       )
               )
       ) 
 group by inn, cutoff;

                                                
/*********************************/
-- Шаг 4. Витрина ПОСТАВЩИКОВ
/*********************************/

create table eks_to_promdm_dan1_sup2_part1 as 
select  inn, 
        cutoff
       ,sum(case when val > 0 then 1 else 0 end) as sup_n
       ,min(round(cnt_dist/6)) as sup_n_6m
       ,round(sum(case when val > 0 and cnt_6m > 0 then 1 else 0 end) / 
             (sum(case when val > 0 then 1 else 0 end) + 0.01), 2)            as sup_rot_6m_cnt
       ,round(sum(case when n <= 3 then val_6m else 0 end)/ (sum(val_6m) + 0.1), 2) as sup_3_big
       ,round(sum(case when n <= 5 then val_6m else 0 end)/ (sum(val_6m) + 0.1), 2) as sup_5_big
	   ,min(case when n = 1 then cp_inn end) as sup_1
       ,round (sum(case when n = 1 then val_6m else 0 end)/ (sum(val_6m) + 0.1), 2) as sup_1_r
       ,min(case when n = 2 then cp_inn end) as sup_2
       ,round(sum(case when n = 2 then val_6m else 0 end)/ (sum(val_6m) + 0.1), 2) as sup_2_r
       ,min(case when n = 3 then cp_inn end) as sup_3
       ,round(sum(case when n = 3 then val_6m else 0 end)/ (sum(val_6m) + 0.1), 2) as sup_3_r
       ,min(case when n = 4 then cp_inn end) as sup_4
       ,round(sum(case when n = 4 then val_6m else 0 end)/ (sum(val_6m) + 0.1), 2) as sup_4_r
       ,min(case when n = 5 then cp_inn end) as sup_5
       ,round(sum(case when n = 5 then val_6m else 0 end)/ (sum(val_6m) + 0.1), 2) as sup_5_r 
  from (select inn, dur, cp_inn, cutoff, cnt_dist, cnt_6m, val, val_6m, 
               ,row_number() over (partition by inn, cutoff order by val_6m desc) as n 
          from (select inn, cp_inn, cutoff, val,
                       sum(case when val > 0 then 1 else 0 end) over (partition by inn order by cutoff desc range 
                       between current row and interval '5' month following) as cnt_dist,
                       sum(val) over (partition by inn, cp_inn order by cutoff desc range 
                       between current row and interval '5' month following) as val_6m,
                       sum(case when val > 0 then 1 else 0 end) over (partition by inn, cp_inn order by cutoff desc range 
                       between interval '1' month following and interval '6' month following) as cnt_6m   
                 from (select t3.inn, t3.cp_inn, t3.cutoff, (case when t4.val is null then 0 else t4.val end) as val 
                         from (select t2.inn, t2.cp_inn, t1.cutoff
                                 from eks_to_promdm_dan1_sup1_part1 t2, 
                                      (select inn, 
                                              trunc(to_date(dt, 'dd.mm.yyyy'), 'month') as cutoff 
                                         from kozhina_es.eks_to_calc_161223_dan 
                                        where SUBTYPE_CODE in ('1_1_12','1_1_13','1_1_15','1_1_16','3_1_5','6_1_6')) 
                                        group by inn, cutoff) t1
                                 where t2.inn = t1.inn
                               ) t3 
                               left join (select inn, 
                                                 cp_inn, 
                                                 trunc(to_date(dt,'dd.mm.yyyy'),'month') as cutoff, 
                                                 sum(val) as val 
                                            from kozhina_es.eks_to_calc_161223_dan 
                                           where SUBTYPE_CODE in ('1_1_12','1_1_13','1_1_15','1_1_16', '3_1_5','6_1_6')
                                           group by inn, cp_inn, cutoff)
                               ) t4
                                on t3.inn = t4.inn 
                               and t3.cp_inn = t4.cp_inn 
                               and t3.cutoff = t4.cutoff
                      -- потестировать исправленный вариант
                       select inn, cp_inn, trunc(to_date(dt,'dd.mm.yyyy'),'month') as cutoff, 
                          sum(val) as val 
                         from kozhina_es.eks_to_calc_161223_dan 
                        where SUBTYPE_CODE in ('1_1_12','1_1_13','1_1_15','1_1_16', '3_1_5','6_1_6')
                        group by inn, cp_inn, cutoff
                      )
               )
       ) 
 group by inn, cutoff;

/*********************************/
-- Шаг 5. Финальная витрина
/*********************************/
create table eks_to_promdm_dan1_fin as
select t1.* 
       ,t2.CUST_N,
	   t2.CUST_N_6M, 
	   t2.CUST_ROT_6M_CNT ,
	   t2.CUST_ROT_6M_SUM ,
	   t2.CUST_1_BIG ,
	   t2.CUST_3_BIG ,
	   t2.CUST_5_BIG,
       t2.CUST_1,
	   t2.CUST_1_R ,
	   t2.CUST_2,
	   t2.CUST_2_R,
	   t2.CUST_3,
	   t2.CUST_3_R,
	   t2.CUST_4,
	   t2.CUST_4_R,
	   t2.CUST_5,
       t2.CUST_5_R,
	   t3.SUP_N ,
	   t3.SUP_N_6M ,
	   t3.SUP_ROT_6M_CNT ,
	   t3.SUP_ROT_6M_SUM, 
	   t3.SUP_1_BIG,
       t3.SUP_3_BIG, 
	   t3.SUP_5_BIG, 
	   t3.SUP_1, 
	   t3.SUP_1_R, 
	   t3.SUP_2,
	   t3.SUP_2_R,
	   t3.SUP_3,
       t3.SUP_3_R,
	   t3.SUP_4, 
	   t3.SUP_4_R, 
	   t3.SUP_5, 
	   t3.SUP_5_R
  from eks_to_promdm_dan1_2_part1 t1 
       left join eks_to_dan1_cust2_part1_2610 t2 on t1.inn = t2.inn and t1.cutoff = t2.cutoff 
       left join eks_to_promdm_dan1_sup2_part1 t3 on t1.inn = t3.inn and t1.cutoff = t3.cutoff;
    
