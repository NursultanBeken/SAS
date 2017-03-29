%let mvConnectToSieb = connect to oracle as sieb(PATH=SIEBEL_PROD USER=SAS_MA  PASSWORD=;
proc sql;
&mvConnectToSieb;
select * from connection to sieb(
select sas_ma.potentional_seq.nextval from dual
);
quit;
