/*****************************************************************************/
/*   loadTableToCAS macros for loading tables from SAS lib to CAS lib in promote or append mode */
/*   	mpSaslib - libname of SAS library from which you want to load data   */
/*   	mpCaslib - CAS library where you want to load data  */
/*   	mpTableName - Please specify table name. By default empty  */
/*   	mode - promote-load the whole new data. append-append to existing data in CAS*/
/*****************************************************************************/

cas mySession sessopts=(caslib=casuser timeout=1800 locale="en_US");

/*DEFINE VARIABLES*/
libname SASLIB '/sasdata/VA/AppData/SASVisualAnalytics/VisualAnalyticsAdministrator/AutoLoad'; /*Please specify libname statement*/
%let mpSaslib=SASLIB;
%let mpCaslib=VA_REPORTS_DATA;
%let mode=promote;
%let mpTableName=; /*Please specify table name. If  you will not specify the name then by default all tables from your library will be loaded*/

%macro loadTableToCAS(mvTableNm,mvSasLib,mvCasLib,mvMode);
	/*load table to inmemory caslib*/
	%if &mvMode=promote %then %do;
		proc casutil;
			droptable casdata="&mvTableNm" incaslib="&mvCasLib" quiet;
		run;
		proc casutil;
			load data=&mvSasLib..&mvTableNm outcaslib="&mvCasLib" 
			casout="&mvTableNm" promote;
		run;
	%end;
	%if &mvMode=append %then %do;
		proc casutil;
			load data=&mvSasLib..&mvTableNm outcaslib="&mvCasLib" casout="&mvTableNm" append;
		run;
		
	%end;
	/*save tables to calib*/
	proc casutil;
    	save casdata="&mvTableNm" incaslib="&mvCasLib" outcaslib="&mvCasLib" replace;
	run;
	
%mend loadTableToCAS;

%macro main;
	/*if table name was not defined then load all tabes in library*/
	%if &mpTableName= %then %do;
		ods output Members=members;
		proc datasets library=&mpSaslib memtype=DATA nodetails;
		run;
		quit;
		
		proc sql noprint;
			select cats('%loadTableToCAS(',name,',&mpSaslib, &mpCaslib , &mode)')
				into :mvExecute separated by ';'
			from work.members;
		quit;
		&mvExecute;
	%end;
	%else %do;
		%loadTableToCAS(&mpTableName,&mpSaslib,&mpCaslib,&mode);
	%end;
%mend main;

%main;
