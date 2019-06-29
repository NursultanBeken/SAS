#!/bin/bash
PACKAGE_DIR=/opt/sas/sashome/SASPlatformObjectFramework/9.4
EXPORT_DIR=<>
LOG_DIR=<>
timestamp="`date "+%Y-%m-%d-%H.%M.%S"`"
PROFILE="SASAdmin"
LOGFILE_NM="$LOG_DIR/di_jobs_meta_bkp_${timestamp}.log"
EMAILADDR=<>
EMAIL_SUBJ="DI jobs metadata backup"

#function for writing to log file
function writetolog () 
{
 echo `date --rfc-3339=seconds`' ' $LOGMSG >> $LOGFILE_NM
}

function exportmeta()
{
	$PACKAGE_DIR/ExportPackage -disableX11 -profile "${PROFILE}" -package "$EXPORT_DIR/${metadata_name}_${timestamp}.spk" -objects "${1}" -subprop "${2}" -types "${3}" -log $LOGFILE_NM
	rc=$?
	if [ $rc -gt 0 ] ; then 
		{  		
			if [ $rc -eq 4 ]; then 
				  LOGMSG="Warning while exporting metadata for ${metadata_name}! View the export log file ${LOGFILE_NM}"; writetolog
          echo -e "Subject:${EMAIL_SUBJ} \n\n Warning while exporting metadata for ${metadata_name}! View the export log file ${LOGFILE_NM}" > ${LOG_DIR}/metadata_bkp_msg.txt
          /usr/sbin/sendmail -f sas@example.com  ${EMAILADDR} < ${LOG_DIR}/metadata_bkp_msg.txt
				else
					LOGMSG="Error while exporting metadata! View the export log file ${LOGFILE_NM}"; writetolog	
          echo -e "Error while exporting metadata for ${metadata_name}!\n For more information, view the export log file ${LOGFILE_NM}" > ${LOG_DIR}/metadata_bkp_msg.txt
          /usr/sbin/sendmail -f sas@example.com  ${EMAILADDR} < ${LOG_DIR}/metadata_bkp_msg.txt
				exit 1
			fi	
		};
		else 
		    LOGMSG="The export of metadata ${metadata_name} has finished successfully"; writetolog
        echo -e "Subject:${EMAIL_SUBJ} \n\n The export of metadata ${metadata_name} has finished successfully" > ${LOG_DIR}/metadata_bkp_msg.txt
        /usr/sbin/sendmail -f sas@example.com  ${EMAILADDR} < ${LOG_DIR}/metadata_bkp_msg.txt  
	fi
}
set -x
unset http_proxy
unset https_proxy
LOGMSG="starting Exporting metadata"; writetolog

metadata_name=dm_jobs; exportmeta "/DWH/ETL/DM(Folder)" "-includeDep" "Job,Table,DeployedFlow"
metadata_name=user_folders; exportmeta "/User Folders(Folder)" "-includeDep" "Job,Table,DeployedFlow"