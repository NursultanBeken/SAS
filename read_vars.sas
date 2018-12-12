proc sql;

select PARAMETER_NAME, VALUE into :mvDParamName1 - :mvDParamName999999, :mvDValue1 - :mvDValue999999
            from connection to postgres (
                select PARAMETER_NAME, VALUE from &mvJParameterTableName 
                where ETL_PROCESS_NAME = %str(%')&mvJobName%str(%')
                );
			%do i=1 %to %eval(&SQLOBS);
				%global &&mvDParamName&i;
				%let &&mvDParamName&i = &&mvDValue&i;
			%end;
			
            %if ^%symexist(mvLastMaxVer) %then %do;
                %let mvStartTimestamp = &mvGlobalCiMinusInf;
            %end;
quit;            
