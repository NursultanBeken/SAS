/****************************************************************************************************
* Program name: LegacyCHtoCDM.sas
* Function:  This macro loads data from legacy contact history 
*	        into SAS CDM tables: ci_campaign,ci_campaign_ext,ci_communication, ci_communication_ext,
*			ci_cell_package, CI_CH_CUSTOMER and CI_CH_CUSTOMER_EXT
* * * mLogProcess -  logs execution process to table job_monitoring
* * * mPrepareDataForCDM - prepares data that should be loaded to CDM tables
* * * mLoadDatatoCDM - loads data from tmp tables to CDM tables
* * * mDeleteTmpTables - deletes all tmp tables created in mPrepareDataForCDM
* Author: Tier One Analytics Inc.
* Creation date: 05JUN2018
* Last update date: 
*****************************************************************************************************/

/*options mprint mlogic symbolgen sastrace=',,,d' sastraceloc=saslog;*/
%macro mLogProcess(mvUpdateFlag=,mvRunId=,mvStep=,mvStatus=);
    %macro d; %mend d;

	%if &mvUpdateFlag=0 %then %do;
        proc sql noprint;
			&mvConnectToSASMA;
			execute (
            	insert into sasma.job_monitoring(RUN_ID,STEP_NAME,START_DTTM)
            	values(&mvRunId,&mvStep,sysdate)
			) by ora;
			disconnect from ora;
        quit;
 		%if &sqlxrc<0 %then %do;
			%goto EXIT;
		%end;
    %end;
    %else %do;
        proc sql noprint;
			&mvConnectToSASMA;
			execute(
            	update sasma.job_monitoring
                	set STEP_STATUS=&mvStatus,FINISH_DTTM=sysdate
            	where RUN_ID=&mvRunId and STEP_NAME=&mvStep
			) by ora;
			disconnect from ora;
        quit;
		%if &sqlxrc<0 %then %do;
			%goto EXIT;
		%end;
    %end; 	 

    %EXIT:

%mend mLogProcess;
%macro mPrepareDataForCDM(mvLibname);	
	/*prepare data that should be loaded to CDM*/
	proc sql noprint;
		create table sasma.ci_campaign_tmp_&mvLibname as
			select 
			    (-10)*log.campaign_nbr as campaign_sk,
				log.campaign_code as campaign_cd,
				"&mvCompleteStatusCD" as campaign_status_cd,
				log.campaign_desc as campaign_nm,
				log.campaign_desc as campaign_desc,
				log.user_id as CAMPAIGN_OWNER_NM,
				log.timestamp as RUN_DTTM,
				log.timestamp as PROCESSED_DTTM,
				%if "&mvLibname" eq "fido" %then %do;
					dim.START_DATE as START_DTTM,
					dim.END_DATE as END_DTTM
				%end;
		from &mvLibname..campaign_log_desc log 
			left join &mvLibname..CAMPAIGN_DIM dim
		on dim.campaign_code=log.campaign_code 
		   and dim.CAMPAIGN_DESCRIPTION=log.CAMPAIGN_DESC
		where log.timestamp > &MONTH_END_DT.;
	quit; 
	%if &SQLRC > 0 %then %do;
		%goto EXIT;
	%end;
	proc sql noprint;
		&mvConnectToSASMA;
		execute (
			grant select on sasma.ci_campaign_tmp_&mvLibname to SASCDM
		) by ora;
		disconnect from ora;
	quit;
	proc sql noprint;
		create table sasma.ci_campaign_ext_tmp_&mvLibname as
			select 
			    (-10)*log.campaign_nbr as campaign_sk,
				log.user_id as MARKETING_MANAGER,
				log.campaign_code as campaign_name,
				log.campaign_type as campaign_type as CAMPAIGN_TYPE_CODE,
				dim.objective||strip(dim.sub_objective) as CAMPAIGN_GROUP_CODE,
				dim.dbm_analyst as CM_ANALYST,
				dim.RECURRING_TYPE as RECURRING_FLAG
		from &mvLibname..campaign_log_desc log 
			left join &mvLibname..CAMPAIGN_DIM dim
			on log.campaign_code=dim.campaign_code
				and dim.CAMPAIGN_DESCRIPTION=log.CAMPAIGN_DESC
			where log.timestamp > &MONTH_END_DT.;
	quit; 
	%if &SQLRC > 0 %then %do;
		%goto EXIT;
	%end;
	proc sql noprint;
		&mvConnectToSASMA;
		execute (
			grant select on sasma.ci_campaign_ext_tmp_&mvLibname to SASCDM
		) by ora;
		disconnect from ora;
	quit;

	proc sql noprint;
		create table sasma.COMMUNICATION_tmp_&mvLibname as
			select 
				(-10)*log.campaign_nbr as COMMUNICATION_SK,
				(-10)*log.campaign_nbr as campaign_sk,
				strip(log.CAMPAIGN_CODE)||strip(put(log.COHORTE,6.))  as COMMUNICATION_CD,
				"&mvExecutedStatusCD" as COMMUNICATION_STATUS_CD,
				case 
					when log.campaign_level='A' then 'Wireless Subsciber'
					when log.campaign_level='S' then 'Wireless Subsciber'
				end as SUBJECT_TYPE_NM,
				log.TIMESTAMP as EXPORT_DTTM,
				log.TIMESTAMP as PROCESSED_DTTM
			from &mvLibname..campaign_log_desc log
				where log.timestamp > &MONTH_END_DT.;
	quit;
	%if &SQLRC > 0 %then %do;
		%goto EXIT;
	%end;
	proc sql noprint;
		&mvConnectToSASMA;
		execute (
			grant select on sasma.COMMUNICATION_TMP_&mvLibname to SASCDM
		) by ora;
		disconnect from ora;
	quit;

	proc sql noprint;
		create table sasma.COMMUNICATION_EXT_tmp_&mvLibname as
			select
				(-10)*log.campaign_nbr as COMMUNICATION_SK,
				log.vendor as VENDOR_CODE,
				log.MEDIA_TYPE as CHANNEL_CODE,
				log.CAMPAIGN_START_DATE as CAMPAIGN_START_DATE,
				log.CAMPAIGN_END_DATE as CAMPAIGN_END_DATE,
				log.FILE_NAME as FILENAME_TEMPLATE,
				log.FILE_PATH as EXPORT_PATH,
				case
					when dim.FRANCHISE_TP='P' then 'I'
					when dim.FRANCHISE_TP='R' then 'O'
				end as LIST_LOB
			from &mvLibname..CAMPAIGN_LOG_DESC log 
				left join &mvLibname..CAMPAIGN_DIM dim
				on log.campaign_code=dim.campaign_code
					and dim.CAMPAIGN_DESCRIPTION=log.CAMPAIGN_DESC
				where log.timestamp > &MONTH_END_DT.;
	quit;
	%if &SQLRC > 0 %then %do;
		%goto EXIT;
	%end;
	proc sql noprint;
		&mvConnectToSASMA;
		execute (
			grant select on sasma.COMMUNICATION_EXT_TMP_&mvLibname to SASCDM
		) by ora;
		disconnect from ora;
	quit;

	proc sql noprint;
		create table sasma.CI_CELL_PACKAGE_tmp_&mvLibname as
			select
				(-10)*log.campaign_nbr+1 as CELL_PACKAGE_SK,
				(-10)*log.campaign_nbr as CAMPAIGN_SK,
				log.campaign_code as campaign_cd,
				log.campaign_desc as campaign_nm,
				(-10)*log.campaign_nbr as COMMUNICATION_SK,
				strip(log.campaign_code)||strip(put(log.cohorte,6.)) as communication_cd,
				log.media_type as channel_cd,
				case 
					when log.campaign_level='A' then 'Wireless Subsciber'
					when log.campaign_level='S' then 'Wireless Subsciber'
				end as SUBJECT_TYPE_NM,
				'_ST' as CONTROL_GROUP_TYPE_CD,
				put(log.TIMESTAMP,DATETIME.) as PROCESSED_DTTM
			from &mvLibname..CAMPAIGN_LOG_DESC log
				where log.timestamp > &MONTH_END_DT.
				and log.control_cnt>0
			union all
				select
				(-10)*log.campaign_nbr as CELL_PACKAGE_SK,
				(-10)*log.campaign_nbr as CAMPAIGN_SK,
				log.campaign_code as campaign_cd,
				log.campaign_desc as campaign_nm,
				(-10)*log.campaign_nbr as COMMUNICATION_SK,
				strip(log.campaign_code)||strip(put(log.cohorte,6.)) as communication_cd,
				log.media_type as channel_cd,
				case 
					when log.campaign_level='A' then 'Wireless Subsciber'
					when log.campaign_level='S' then 'Wireless Subsciber'
				end as SUBJECT_TYPE_NM,
				"" as CONTROL_GROUP_TYPE_CD,
				put(log.TIMESTAMP,DATETIME.) as PROCESSED_DTTM
			from &mvLibname..CAMPAIGN_LOG_DESC log
				where log.timestamp > &MONTH_END_DT.
				and log.target_cnt>0;
	quit;
	%if &SQLRC > 0 %then %do;
		%goto EXIT;
	%end;
	proc sql noprint;
		&mvConnectToSASMA;
		execute (
			grant select on sasma.CI_CELL_PACKAGE_tmp_&mvLibname to SASCDM
		) by ora;
		disconnect from ora;
	quit;
	%if &sqlxrc<0 %then %do;
		%goto EXIT;
	%end;
	/*finished preparing data for CDM*/
	%EXIT:
%mend mPrepareDataForCDM;
%macro mLoadDatatoCDM(mvLibname);
	%macro dummy; %mend dummy;
	proc sql noprint;
		&mvConnectToCDM;
		execute (
			BEGIN
				insert into &CDM_SCHEMA..ci_campaign(	CAMPAIGN_SK,
					CAMPAIGN_CD,
					CAMPAIGN_STATUS_CD,
					CAMPAIGN_NM,
					CAMPAIGN_DESC,
					CAMPAIGN_OWNER_NM,
					RUN_DTTM,
					PROCESSED_DTTM,
					START_DTTM,
					END_DTTM
				) 
				select 
					CAMPAIGN_SK,
					CAMPAIGN_CD,
					CAMPAIGN_STATUS_CD,
					CAMPAIGN_NM,
					CAMPAIGN_DESC,
					CAMPAIGN_OWNER_NM,
					RUN_DTTM,
					PROCESSED_DTTM,
					%if "&mvLibname" eq "fido" %then %do;
						START_DTTM,
						END_DTTM
					%end;
					%else %do;
						null,
						null
					%end;
				from &SASMA_SCHEMA..ci_campaign_tmp_&mvLibname;
						
				insert into &CDM_SCHEMA..ci_campaign_ext(
					CAMPAIGN_SK,
					MARKETING_MANAGER,
					CAMPAIGN_NAME,
					CAMPAIGN_TYPE_CODE,
					CAMPAIGN_GROUP_CODE,
					CM_ANALYST,
					RECURRING_FLAG
				)
				select * from &SASMA_SCHEMA..ci_campaign_ext_tmp_&mvLibname;
				
				
				insert into &CDM_SCHEMA..CI_COMMUNICATION(
					COMMUNICATION_SK,
					CAMPAIGN_SK,
					COMMUNICATION_CD,
					COMMUNICATION_STATUS_CD,
					SUBJECT_TYPE_NM,
					EXPORT_DTTM,
					PROCESSED_DTTM
				)
				select * from &SASMA_SCHEMA..COMMUNICATION_tmp_&mvLibname;
							
				insert into &CDM_SCHEMA..CI_COMMUNICATION_EXT(
					COMMUNICATION_SK,
					VENDOR_CODE,
					CHANNEL_CODE,
					CAMPAIGN_START_DATE,
					CAMPAIGN_END_DATE,
					FILENAME_TEMPLATE,
					EXPORT_PATH,
					LIST_LOB
				)
				select * from &SASMA_SCHEMA..COMMUNICATION_EXT_TMP_&mvLibname;
								
				insert into &CDM_SCHEMA..CI_CELL_PACKAGE(
					CELL_PACKAGE_SK,
					CAMPAIGN_SK,
					CAMPAIGN_CD,
					CAMPAIGN_NM,
					COMMUNICATION_SK,
					COMMUNICATION_CD,
					CHANNEL_CD,
					SUBJECT_TYPE_NM,
					CONTROL_GROUP_TYPE_CD,
					PROCESSED_DTTM
				)				
				select 
					CELL_PACKAGE_SK,
					CAMPAIGN_SK,
					CAMPAIGN_CD,
					CAMPAIGN_NM,
					COMMUNICATION_SK,
					COMMUNICATION_CD,
					CHANNEL_CD,
					SUBJECT_TYPE_NM,
					CONTROL_GROUP_TYPE_CD,
					to_date(PROCESSED_DTTM,'dd-mon-yy hh24:mi:ss')
				from &SASMA_SCHEMA..CI_CELL_PACKAGE_tmp_&mvLibname;									
			
				COMMIT;
			end;/*if there is an error all transaction will be rolled back*/
		) by ora;
		disconnect from ora;
	quit;
	%if &sqlxrc < 0 %then %do;
		%mLogProcess(mvUpdateFlag=1,mvRunId=&mvCurRunId,mvStep='LOAD DATA TO CDM',mvStatus='ERROR');
		%goto EXIT;
	%end;
	%EXIT:

%mend mLoadDatatoCDM;
%macro mDeleteTmpTables(mvLibname);
	%if %sysfunc(exist(&SASMA_SCHEMA..CI_CELL_PACKAGE_tmp_&mvLibname)) %then %do;
			proc delete data=&SASMA_SCHEMA..CI_CELL_PACKAGE_tmp_&mvLibname;
			run;
	%end;
	%if %sysfunc(exist(&SASMA_SCHEMA..COMMUNICATION_EXT_TMP_&mvLibname)) %then %do;
			proc delete data=&SASMA_SCHEMA..COMMUNICATION_EXT_TMP_&mvLibname;
			run;
	%end;
	%if %sysfunc(exist(&SASMA_SCHEMA..COMMUNICATION_TMP_&mvLibname)) %then %do;
			proc delete data=&SASMA_SCHEMA..COMMUNICATION_TMP_&mvLibname;
			run;
		%end;
		%if %sysfunc(exist(&SASMA_SCHEMA..ci_campaign_ext_tmp_&mvLibname)) %then %do;
			proc delete data=&SASMA_SCHEMA..ci_campaign_ext_tmp_&mvLibname;
			run;
		%end;
		%if %sysfunc(exist(&SASMA_SCHEMA..ci_campaign_tmp_&mvLibname)) %then %do;
			proc delete data=&SASMA_SCHEMA..ci_campaign_tmp_&mvLibname;
			run;
		%end; 
%mend mDeleteTmpTables;

%macro  mLegacyCHtoCDM;
	%macro d; %mend d;
	%let DEBUG=0;
	%let mvRetentionPeriod = 1; /*in months*/
	%let CDMpass={SAS002}97DEDA1858B7EFFC2FEA60F9359B9A361BC74FFB50B8230D;
	%let SASMApass={SAS002}2232812D5C2EA96D50A6E486202461DF;

	/* assign libraries and connections */
	%let rogers_dir=/apps/data/SASMA/legacy_ch_data/rogers/;
	%let fido_dir=/apps/data/SASMA/legacy_ch_data/fido/;
	libname rogers "&rogers_dir";
	libname fido "&fido_dir"; 
	libname sasma oracle user=SASMA password="&SASMApass" path=CMTMADEV;
	libname sascdm oracle user=SASCDM PASSWORD="&CDMpass" PATH=CMTMADEV;	
	%let mvConnectToCDM = connect to oracle as ora(PATH=CMTMADEV USER=sascdm  PASSWORD="&CDMpass");
	%let mvConnectToSASMA= connect to oracle as ora(PATH=CMTMADEV USER=sasma  PASSWORD="&SASMApass");
	%let CDM_SCHEMA = sascdm;
	%let SASMA_SCHEMA = sasma;
	%let mvfido=fido;
	%let mvrogers=rogers;


	/*get (current date - retention period)*/
	%LET DATE=%SYSFUNC(DATETIME());
    %LET MONTH_END_DT = %SYSFUNC(INTNX(DTMONTH, &DATE., -&mvRetentionPeriod.,same));

	/* get current run id for loging process*/
	%local mvCurRunId;
	proc sql noprint;
	select COALESCE(max(run_id),0) + 1 into: mvCurRunId 
		from sasma.job_monitoring;
	quit;

	%local mvCompleteStatusCD mvExecutedStatusCD;
	proc sql noprint;
		select CAMPAIGN_STATUS_CD into: mvCompleteStatusCD
			from sascdm.CI_CAMPAIGN_STATUS
			where CAMPAIGN_STATUS_DESC = 'Complete';
	quit;
	%if &SQLRC > 0 %then %do;
		%goto EXIT;
	%end;
	proc sql noprint;
		select communication_status_cd into: mvExecutedStatusCD 
			from sascdm.ci_communication_status
			where communication_status_desc='Executed';
	quit;
	%if &SQLRC > 0 %then %do;
		%goto EXIT;
	%end;	

	/*truncate staging tables*/
	%mLogProcess(mvUpdateFlag=0,mvRunId=&mvCurRunId,mvStep='TRUNCATE STG TABLES');
	proc sql noprint;
		&mvConnectToCDM;
		execute (truncate table &CDM_SCHEMA..CI_CH_CUSTOMER_EXT_STG) by ora;
		disconnect from ora;
	quit;
	%if &sqlxrc<0 %then %do;
		%goto EXIT;
	%end;
	proc sql noprint;
		&mvConnectToCDM;
		execute (truncate table &CDM_SCHEMA..CI_CH_CUSTOMER_STG) by ora;
		disconnect from ora;
	quit;	
	%if &sqlxrc<0 %then %do;
		%goto EXIT;
	%end;
	%mLogProcess(mvUpdateFlag=1,mvRunId=&mvCurRunId,mvStep='TRUNCATE STG TABLES',mvStatus='OK');

	/*reset sequence*/
	proc sql noprint;
		&mvConnectToSASMA;;
		execute(drop sequence &SASMA_SCHEMA..SEQ_LEGACY_EXT_ID) by ora;
		execute(create sequence &SASMA_SCHEMA..SEQ_LEGACY_EXT_ID) by ora;
	quit;
	%if &sqlxrc<0 %then %do;
		%goto EXIT;
	%end;

	/*prepare data to staging layer*/
	proc sql noprint;
		create table sasma.fido_stg_tmp as 
		select
			(-10)*lg.CAMPAIGN_NBR as CELL_PACKAGE_SK,
			ld.TIMESTAMP as CONTACT_DTTM,
			"&mvExecutedStatusCD" as CONTACT_HISTORY_STATUS_CD,
			lg.ban as ban,
			ld.CAMPAIGN_LEVEL as SUBJ_LEVEL,
			"LEGACY" as DATA_SOURCE,
			ld.CAMPAIGN_CODE,
			ld.CYCLE as campaign_cycle,
			ld.CAMPAIGN_DESC ,
			ld.CAMPAIGN_START_DATE,
			ld.CAMPAIGN_END_DATE,
			ld.CAMPAIGN_TYPE,
			lg.CELL_CD,
			ld.COHORTE,
			lg.MSID,
			lg.OFFER_CODE,
			lg.SAMPLE_TYPE
		from fido.CAMPAIGN_LOG lg 
				left join fido.CAMPAIGN_LOG_DESC ld
				on ld.CAMPAIGN_NBR=lg.CAMPAIGN_NBR
			where ld.timestamp > &MONTH_END_DT.;
	quit;
	%if &SQLRC > 0 %then %do;
		%goto EXIT;
	%end;
	proc sql noprint;
		create table sasma.rogers_stg_tmp as 
		select
			(-10)*lg.CAMPAIGN_NBR as CELL_PACKAGE_SK,
			ld.TIMESTAMP as CONTACT_DTTM,
			"&mvExecutedStatusCD" as CONTACT_HISTORY_STATUS_CD,
			lg.ban as ban,
			ld.CAMPAIGN_LEVEL as SUBJ_LEVEL,
			"LEGACY" as DATA_SOURCE,
			ld.CAMPAIGN_CODE,
			ld.CYCLE as campaign_cycle,
			ld.CAMPAIGN_DESC ,
			ld.CAMPAIGN_START_DATE,
			ld.CAMPAIGN_END_DATE,
			ld.CAMPAIGN_TYPE,
			lg.CELL_CD,
			ld.COHORTE,
			lg.MSID,
			lg.OFFER_CODE,
			lg.SAMPLE_TYPE
		from rogers.CAMPAIGN_LOG lg 
				left join rogers.CAMPAIGN_LOG_DESC ld
				on ld.CAMPAIGN_NBR=lg.CAMPAIGN_NBR
			where ld.timestamp > &MONTH_END_DT.;
	quit;
	%if &SQLRC > 0 %then %do;
		%goto EXIT;
	%end;
	/* union of data from rogers and fido */
	proc sql noprint;
		&mvConnectToSASMA;
		execute (
			create table sasma.CI_CH_CUSTOMER_stg_tmp as 
				select 
				CELL_PACKAGE_SK,               
				CONTACT_DTTM,                  
				CONTACT_HISTORY_STATUS_CD ,    
				'LEGACY'||to_char(SEQ_LEGACY_EXT_ID.nextval) as EXTERNAL_CONTACT_INFO_ID1,
				BAN,                       
				SUBJ_LEVEL,                   
				DATA_SOURCE,                
				CAMPAIGN_CODE,               
				CAMPAIGN_CYCLE,               
				CAMPAIGN_DESC,                
				CAMPAIGN_START_DATE,          
				CAMPAIGN_END_DATE,           
				CAMPAIGN_TYPE,               
				CELL_CD,                   
				COHORTE,                     
				MSID,                       
				OFFER_CODE,                   
				SAMPLE_TYPE
			from
				(select * from sasma.fido_stg_tmp
					union all
				select * from sasma.rogers_stg_tmp		
				)T
		) by ora;
		disconnect from ora;	
	quit;
	%if &sqlxrc<0 %then %do;
		%goto EXIT;
	%end;
	proc sql noprint;
		&mvConnectToSASMA;
		execute (
			grant select on sasma.CI_CH_CUSTOMER_stg_tmp to SASCDM
		) by ora;
		disconnect from ora;
	quit;
	%if &sqlxrc<0 %then %do;
		%goto EXIT;
	%end;

	/*load data to staging layer*/	
	%mLogProcess(mvUpdateFlag=0,mvRunId=&mvCurRunId,mvStep='LOAD TO STG');
	proc sql noprint;
		&mvConnectToCDM;
		execute (
			insert /*+ APPEND */ into sascdm.CI_CH_CUSTOMER_STG(
				CELL_PACKAGE_SK,
				CONTACT_DTTM,
				CONTACT_DT,
				CONTACT_HISTORY_STATUS_CD,
				EXTERNAL_CONTACT_INFO_ID1,
				BAN,
				SUBJ_LEVEL,
				RSID_WX,
				DATA_SOURCE
				)
			select
				CELL_PACKAGE_SK,
				CONTACT_DTTM,
				trunc(CONTACT_DTTM),
				CONTACT_HISTORY_STATUS_CD,
				EXTERNAL_CONTACT_INFO_ID1,
				BAN,
				SUBJ_LEVEL,
				to_char(BAN)||to_char(MSID),
				DATA_SOURCE
			from sasma.CI_CH_CUSTOMER_stg_tmp
		) by ora ;
		disconnect from ora;
	quit;
	%if &sqlxrc<0 %then %do;
		%PUT ERROR: *****************************************************************************;
   		%PUT ERROR: Data was not loaded into CI_CH_CUSTOMER_STG;
   		%PUT ERROR: *****************************************************************************;
		%goto EXIT;
	%end;
	%local mvStgCustCheck;
	proc sql noprint;
		select count(*) into:mvStgCustCheck from sascdm.CI_CH_CUSTOMER_STG;
	quit;
	%if &SQLRC > 0 %then %do;
		%goto EXIT;
	%end;
	/*check if STG is empty*/
	%if &mvStgCustCheck = 0 %then %do;
		%PUT ERROR: *****************************************************************************;
   		%PUT ERROR: No records found in CI_CH_CUSTOMER_STG;
   		%PUT ERROR: *****************************************************************************;
		%mLogProcess(mvUpdateFlag=1,mvRunId=&mvCurRunId,mvStep='LOAD TO STG',mvStatus='ERROR');
		%goto EXIT;
	%end;
	proc sql noprint;
		&mvConnectToCDM;
		execute (
			insert /*+ APPEND */ into sascdm.CI_CH_CUSTOMER_EXT_STG(
				ID,
				CAMPAIGN_CODE,
				CAMPAIGN_CYCLE,
				CAMPAIGN_DESCRIPTION,
				CAMPAIGN_START_DATE,
				CAMPAIGN_END_DATE,
				CAMPAIGN_NAME,
				CAMPAIGN_TYPE,
				CELL_CD,
				COHORTE,
				CTN,
				OFFER_CD,
				SAMPLE_TYPE,
				RSID,
				BAN,
				CONTACT_DTTM,
				DATA_SOURCE
			)
			select
				EXTERNAL_CONTACT_INFO_ID1,
				CAMPAIGN_CODE,
				CAMPAIGN_CYCLE,
				CAMPAIGN_DESC,
				CAMPAIGN_START_DATE,
				CAMPAIGN_END_DATE,
				CAMPAIGN_DESC,
				CAMPAIGN_TYPE,
				CELL_CD,
				COHORTE,
				MSID,
				OFFER_CODE,
				SAMPLE_TYPE,
				to_char(BAN)||to_char(MSID),
				BAN,
				CONTACT_DTTM,
				DATA_SOURCE
			from sasma.CI_CH_CUSTOMER_stg_tmp
		) by ora;
		disconnect from ora;
	quit;
	%if &sqlxrc<0 %then %do;
		%PUT ERROR: *****************************************************************************;
   		%PUT ERROR: Data was not loaded into CI_CH_CUSTOMER_EXT_STG;
   		%PUT ERROR: *****************************************************************************;
		%goto EXIT;
	%end;

	%local mvStgCustExtCheck;
	proc sql noprint;
		select count(*) into:mvStgCustExtCheck from sascdm.CI_CH_CUSTOMER_EXT_STG;
	quit;
	%if &SQLRC > 0 %then %do;
		%goto EXIT;
	%end;
	/*check if STG is empty*/
	%if &mvStgCustExtCheck = 0 %then %do;
		%PUT ERROR: *****************************************************************************;
   		%PUT ERROR: No records found in CI_CH_CUSTOMER_EXT_STG;
   		%PUT ERROR: *****************************************************************************;
		%mLogProcess(mvUpdateFlag=1,mvRunId=&mvCurRunId,mvStep='LOAD TO STG',mvStatus='ERROR');
		%goto EXIT;
	%end;
	%mLogProcess(mvUpdateFlag=1,mvRunId=&mvCurRunId,mvStep='LOAD TO STG',mvStatus='OK');
	/*finished loading data to staging layer*/

	/*prepare data that should be loaded to CDM*/
	%mPrepareDataForCDM(&mvfido);
	%mPrepareDataForCDM(&mvrogers);

	/*delete old data from CDM tables*/
	%mLogProcess(mvUpdateFlag=0,mvRunId=&mvCurRunId,mvStep='DELETE FROM CDM');
	proc sql noprint;
		&mvConnectToCDM;
		execute (
			BEGIN
				delete from &CDM_SCHEMA..CI_CELL_PACKAGE where CELL_PACKAGE_SK<0;
				delete from &CDM_SCHEMA..CI_COMMUNICATION_EXT where COMMUNICATION_SK<0;
				delete from &CDM_SCHEMA..CI_COMMUNICATION where COMMUNICATION_SK<0;
				delete from &CDM_SCHEMA..ci_campaign_ext where CAMPAIGN_SK<0;
				delete from &CDM_SCHEMA..ci_campaign where CAMPAIGN_SK<0;

				commit;
			end;/*if there is an error all transaction will be rolled back*/
		) by ora;
		disconnect from ora;
	quit;
	%if &sqlxrc<0 %then %do;
		%mLogProcess(mvUpdateFlag=1,mvRunId=&mvCurRunId,mvStep='DELETE FROM CDM',mvStatus='ERROR');
		%goto EXIT;
	%end;
	%mLogProcess(mvUpdateFlag=1,mvRunId=&mvCurRunId,mvStep='DELETE FROM CDM',mvStatus='OK');
	
	/*load data to CDM*/
	%mLogProcess(mvUpdateFlag=0,mvRunId=&mvCurRunId,mvStep='LOAD DATA TO CDM');
	%mLoadDatatoCDM(&mvfido);
	%mLoadDatatoCDM(&mvrogers);
	%mLogProcess(mvUpdateFlag=1,mvRunId=&mvCurRunId,mvStep='LOAD DATA TO CDM',mvStatus='OK');
	
	/*exchange partition*/
	%mLogProcess(mvUpdateFlag=0,mvRunId=&mvCurRunId,mvStep='EXCHANGE PARTITION');
	proc sql noprint;
		&mvConnectToCDM;
		execute(
			  ALTER TABLE CI_CH_CUSTOMER
				EXCHANGE PARTITION FROM_LEGACY
				WITH TABLE CI_CH_CUSTOMER_STG
				UPDATE GLOBAL INDEXES
		) by ora;
		disconnect from ora;	
	quit;
	%if &sqlxrc < 0 %then %do;
		%mLogProcess(mvUpdateFlag=1,mvRunId=&mvCurRunId,mvStep='EXCHANGE PARTITION',mvStatus='ERROR');
		%goto EXIT;
	%end;
	proc sql noprint;
		&mvConnectToCDM;
		execute(
			  ALTER TABLE CI_CH_CUSTOMER_EXT
				EXCHANGE PARTITION FROM_LEGACY_EXT
				WITH TABLE CI_CH_CUSTOMER_EXT_STG
				UPDATE GLOBAL INDEXES
		) by ora;	
		disconnect from ora;		
	quit;
	%if &sqlxrc < 0 %then %do;
		%mLogProcess(mvUpdateFlag=1,mvRunId=&mvCurRunId,mvStep='EXCHANGE PARTITION',mvStatus='ERROR');
		%goto EXIT;
	%end;
	%mLogProcess(mvUpdateFlag=1,mvRunId=&mvCurRunId,mvStep='EXCHANGE PARTITION',mvStatus='OK');

	%EXIT:
	%if &DEBUG=0 %then %do;	
		%if %sysfunc(exist(&SASMA_SCHEMA..CI_CH_CUSTOMER_stg_tmp)) %then %do;
			proc delete data=&SASMA_SCHEMA..CI_CH_CUSTOMER_stg_tmp;
			run;
		%end;
		%if %sysfunc(exist(&SASMA_SCHEMA..fido_stg_tmp)) %then %do;
			proc delete data=&SASMA_SCHEMA..fido_stg_tmp;
			run;
		%end;
		%if %sysfunc(exist(&SASMA_SCHEMA..rogers_stg_tmp)) %then %do;
			proc delete data=&SASMA_SCHEMA..rogers_stg_tmp;
			run;
		%end;
		%mDeleteTmpTables(&mvfido);
		%mDeleteTmpTables(&mvrogers);
	%end;	
	libname rogers clear; 
	libname fido clear;

%mend mLegacyCHtoCDM;
%mLegacyCHtoCDM;