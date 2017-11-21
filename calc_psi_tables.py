from __future__ import division
import json
import sys
import numpy as np
import pandas as pd
from time import time
from math import *
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
import re


# calculate population stabillity index 
# expected - etalon mounth stat
# actual - current mounth stat
# qlist - list of quantiles for calculating
def calc_psi(expected, actual, qlist):
        expected_len=expected.count()
        actual_len=actual.count()
        psi_tot=0
        for j in range(len(qlist)-1):
            actual_pct=actual.filter((actual.SCORE>=qlist[j]) & (actual.SCORE<qlist[j+1])).count() / actual_len
            expected_pct=expected.filter((expected.SCORE>=qlist[j]) & (expected.SCORE<qlist[j+1])).count() / expected_len
            if (actual_pct>0) and (expected_pct>0):
                psi_cut = (expected_pct-actual_pct)*np.log(expected_pct/actual_pct)
                psi_tot+=psi_cut
        return np.round(psi_tot,7)


if __name__ == "__main__":

	current_month = str(sys.argv[1])
	print ('Current_month:', current_month)
	num2round = 5   # precision
	groups_count=10 # group counts to calculate PSI

	table_list=[
		'5_skb_bee_rf_so_v1_',
		'6_skb_comp_rf_so_v1_',
		'11_home_bee_xgb_home_v1_',
		'13_rb_bee_rf_oss_ver1_',
		'14_rb_comp_rf_oss_ver1_',
		'16_tks_pos_bee_lr_so_v4_',
		'16_tks_cc_bee_lr_so_v4_'
		]
	month_list=[
		'201703',
		'201703',
		'201704',
		'201706',
		'201707',
		'201708',
		'201702'
		]

	# maincycle
	t0=time()
	statDF = []
	tks_psiDF = []
	
	sc = SparkContext()
	hc = HiveContext(sc)
	
	for i in range(len(table_list)):
	    print ('**************', table_list[i], '\t', '**************', np.round((time()-t0)/60.,3))
	    cur_table=table_list[i]+current_month
	    init_table = table_list[i]+month_list[i]
	    
	    #1. rec count
	    SCR = hc.sql('select * from arstel.%s' %cur_table)
	    RecCount = SCR.count()
	    #print 'rec_count: ', RecCount
	    
	    #2. subs_key distinct
	    subs_key_distinct = SCR.select('subs_key').distinct().count()
	    #print 'subs_key distinct: ',  subs_key_distinct
	    
	    #3. null count
	    NullCount = SCR.filter(SCR['score_ball'].isNull()).count()\
	                        +  SCR.filter(SCR['score_ball']=='')\
	                        .count()
	    #print 'null count: ', NullCount
	    
	    #4. max, min, avg score    
	    if 'tks' not in cur_table:
	        SCRstat = SCR.withColumn('SCORE_DOUBLE', regexp_replace('score_ball', ',', '.').cast("double"))
	    else:   # only by last 7 digits     
	        SCRstat = SCR\
	                .withColumn('TSCORE', SCR.score_ball.substr(8,6))\
	                .withColumn('TSCORE_SIGN', SCR.score_ball.substr(7,1))\
	                .withColumn("SCORE",when(col("TSCORE_SIGN")=='9', -1*col("TSCORE")).otherwise(col("TSCORE")))\
	                .withColumn("SCORE_DOUBLE1",col("SCORE").cast("double"))\
	                .withColumn("SCORE_DOUBLE",col("SCORE")/100000)\
	                .select("SCORE_DOUBLE").cache()
	        
	    score_max = np.round(SCRstat.agg({"SCORE_DOUBLE": "max"}).collect()[0][0],num2round)
	    score_min = np.round(SCRstat.agg({"SCORE_DOUBLE": "min"}).collect()[0][0],num2round)
	    score_avg = np.round(SCRstat.agg({"SCORE_DOUBLE": "avg"}).collect()[0][0],num2round)
	    #print 'max: ', score_max, 'min: ', score_min, 'avg: ', score_avg
	    
	    #5. PSI
	    if 'tks' not in cur_table:
	        expected = hc.sql("SELECT regexp_replace(score_ball,',','.') as SCORE from arstel.%s order by SCORE" %init_table).cache()
	        actual = hc.sql("SELECT regexp_replace(score_ball,',','.') as SCORE from arstel.%s order by SCORE" %cur_table).cache()
	        qlist = list(np.linspace(0,1,groups_count+1))
	        psi_tot = calc_psi(expected, actual, qlist)
	        print ('psi: ', psi_tot)
	    else:
	    	#calc psi separatly for each of 5 (POS  [1:5]) or 6 (CC [:5] ) digits
	        qlist = list(np.linspace(0,10,groups_count+1))
	        tks_psi=['']*6
	        eSCR = hc.sql('select * from arstel.%s' %init_table).cache()
	        aSCR = hc.sql('select * from arstel.%s' %cur_table).cache()
	        for i_tks in range(6): # for all single digit separatelly
	            expected = eSCR.withColumn('SCORE', eSCR.score_ball.substr(i_tks+1,1))\
	                                .select('SCORE')\
	                                .sort(asc('SCORE'))\
	                                .cache()
	            actual =   aSCR.withColumn('SCORE', aSCR.score_ball.substr(i_tks+1,1))\
	                                .select('SCORE')\
	                                .sort(asc('SCORE'))\
	                                .cache()       
	                
	        
	            tks_psi[i_tks] = calc_psi(expected, actual, qlist)            
	            #print 'psi parth: ', tks_psi[i_tks]
	        tks_psiDF.append([table_list[i], tks_psi[0], tks_psi[1], tks_psi[2], tks_psi[3], tks_psi[4], tks_psi[5]])
	        
	        # separatly calc for entire sccore-ball (last 7 digits) 
	        expected = eSCR\
	        .withColumn('TSCORE', eSCR.score_ball.substr(8,6).cast("double"))\
	        .withColumn('TSCORE_SIGN', eSCR.score_ball.substr(7,1))\
	        .withColumn("TSCORE2",when(col("TSCORE_SIGN")=='9', -1*col("TSCORE")).otherwise(col("TSCORE")))\
	        .withColumn("SCORE",(1000000+col("TSCORE2"))/2000000)\
	        .select('SCORE')\
	        .cache()
	        
	        actual = aSCR\
	        .withColumn('TSCORE', aSCR.score_ball.substr(8,6).cast("double"))\
	        .withColumn('TSCORE_SIGN', aSCR.score_ball.substr(7,1))\
	        .withColumn("TSCORE2",when(col("TSCORE_SIGN")=='9', -1*col("TSCORE")).otherwise(col("TSCORE")))\
	        .withColumn("SCORE",(1000000+col("TSCORE2"))/2000000)\
	        .select('SCORE')\
	        .cache()
	        
	        qlist = list(np.linspace(0,1,groups_count+1))
	        psi_tot = calc_psi(expected, actual, qlist)
	        print ('psi tot: ', psi_tot)
	    
	    statDF.append([table_list[i], 
	                       RecCount,
	                       subs_key_distinct,
	                       NullCount,
	                       score_max,
	                       score_min,
	                       score_avg,
	                       psi_tot]) 

	# save psi's
	statDFpd = pd.DataFrame(statDF, columns=('table', 'RecCount','subs_key_distinct','NullCount','score_max','score_min','score_avg','psi'))
	tks_psiPOS = pd.DataFrame([tks_psiDF[0][2:]], columns=('life_time_bin', 'unic_ctn_cpa_1m_bin','minus_bal_day_avg_hy_bin',
	                                                       'class_device_bin','call_charge_ind_6m_bin'))
	tks_psiCC = pd.DataFrame([tks_psiDF[1][1:]], 
	columns=('life_time_bounded_bin', 'minus_bal_day_avg_hy_bounded_ln_bin','unic_ctn_cpa_1m_bounded_ln_bin',
	         'pay_charge_ind_1m_bin','class_device_bin','call_charge_ind_1m_bin'))

	writer = pd.ExcelWriter('PSI_CALCULATE'+current_month+'.xlsx')
	statDFpd.to_excel(writer,'ALL')
	tks_psiPOS.T.to_excel(writer,'TinkoffPOS')
	tks_psiCC.T.to_excel(writer,'TinkoffCC')
	writer.save()
