%let mvConnectToSieb = connect to oracle as sieb(PATH=SIEBEL_PROD USER=SAS_MA  PASSWORD="{SAS002}2FA3912649F90F17002362BC37AEBB8B");
proc sql;
&mvConnectToSieb;
select * from connection to sieb(
select sas_ma.potentional_seq.nextval from dual
);
quit;