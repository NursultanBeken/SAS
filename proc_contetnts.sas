/*lists only the names, types, sizes, and modification dates for the SAS files in the worklib library*/
libname worklib '/temp/SAS_work2293000045FD_sas-app-prod-02';
proc contents data=worklib._all_ nods;
run;