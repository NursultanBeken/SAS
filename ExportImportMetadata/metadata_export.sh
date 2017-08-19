#!/bin/bash
PACKAGE_DIR=/sas/SASHome/SASPlatformObjectFramework/9.4
EXPORT_DIR=/temp/preProdSync/metadata
LOG_DIR=/logs/export_spk_logs
EMAILADDR="nursultan.bekenov@glowbyteconsulting.com"
EMAIL_SUBJ="BIN_BANK PreprodSynchronization"
timestamp="`date "+%Y-%m-%d-%H.%M.%S"`"
RUNNINGINSTANCE=$(uuidgen)
MAINLOGFILE="$LOG_DIR/PreprodSynchronization_${timestamp}.log"
PROFILE="TEST migr"
function writetolog () 
{
 echo `date --rfc-3339=seconds`' ' $LOGMSG >> $MAINLOGFILE
}
LOGMSG="This is metadata synchronization process"; writetolog
LOGMSG="starting Exporting metadata"; writetolog
#1 export roles groups and users 
LOGMSG="starting exporting exporting groupes, roles and users ..."; writetolog
script=ExportSecurity
LOGFILE="$LOG_DIR/${script}_${timestamp}.log"
$PACKAGE_DIR/ExportPackage -profile "${PROFILE}" -package "$EXPORT_DIR/security.spk" -objects "/System/Security(Folder)" -subprop -includeDep -log $LOGFILE
rc=$?
if [ $rc -gt 0 ] ; then 
	{  		
		if [ $rc -eq 4 ]; then 
			LOGMSG="Warning while exporting infomap! View the export log file ${LOGFILE}"; writetolog
		else
			LOGMSG="Error while exporting groupes, roles and users! View the export log file ${LOGFILE}"; writetolog	
			echo -e "Error while exporting groupes, roles and users!\n For more information, view the export log file ${LOGFILE}" | mailx -s "${EMAIL_SUBJ}" $EMAILADDR
			exit 1
		fi	
	};
	else 
	LOGMSG="The export of groupes, roles and users has finished successfully"; writetolog
fi
#2 export Infomap
LOGMSG="starting exporting exporting infomap ... "; writetolog
script=ExportInfomap
LOGFILE="$LOG_DIR/${script}_${timestamp}.log"
$PACKAGE_DIR/ExportPackage -profile "${PROFILE}" -package "$EXPORT_DIR/infomap.spk" -objects "/RB Marketing Automation/Information Maps/rb_marketing(InformationMap)" -subprop -log $LOGFILE
rc=$?
if [ $rc -gt 0 ] ; then 
	{ 
		if [ $rc -eq 4 ]; then 
			LOGMSG="Warning while exporting infomap! View the export log file ${LOGFILE}"; writetolog
		else
			LOGMSG="Error while exporting infomap! View the export log file ${LOGFILE}"; writetolog
			echo -e "Error while exporting infomap!\n For more information, view the export log file ${LOGFILE}" | mailx -s "${EMAIL_SUBJ}" $EMAILADDR
			exit 1
		fi
	};
	else 
	LOGMSG="The export of infomap has finished successfully"; writetolog
fi
#3 export Campaign deffinition
LOGMSG="starting exporting exporting Campaign deffinition ... "; writetolog
script=ExportCampaignDef
LOGFILE="$LOG_DIR/${script}_${timestamp}.log"
$PACKAGE_DIR/ExportPackage -profile "${PROFILE}" -package "$EXPORT_DIR/campaignDef.spk" -objects "/RB Marketing Automation/Business Contexts/Retail Bank - Marketing/Campaign Definitions(Folder)" -subprop -log $LOGFILE
rc=$?
if [ $rc -gt 0 ] ; then 
	{
		if [ $rc -eq 4 ]; then 
			LOGMSG="Warning while exporting Campaign deffinition! View the export log file ${LOGFILE}"; writetolog
		else	
			LOGMSG="Error while exporting Campaign deffinition! View the export log file ${LOGFILE}"; writetolog
			echo -e "Error while exporting Campaign deffinition!\n For more information, view the export log file ${LOGFILE}" | mailx -s "${EMAIL_SUBJ}" $EMAILADDR
			exit 1
		fi
	};
	else 
	LOGMSG="The export of Campaign deffinition has finished successfully"; writetolog
fi
#4 export Export deffinition
LOGMSG="starting exporting exporting Export deffinition ... "; writetolog
script=ExportExportDef
LOGFILE="$LOG_DIR/${script}_${timestamp}.log"
$PACKAGE_DIR/ExportPackage -profile "${PROFILE}" -package "$EXPORT_DIR/exportDef.spk" -objects "/RB Marketing Automation/Business Contexts/Retail Bank - Marketing/Export Definitions(Folder)" -subprop -log $LOGFILE
rc=$?
if [ $rc -gt 0 ] ; then 
	{
		if [ $rc -eq 4 ]; then 
			LOGMSG="Warning while exporting Export deffinition! View the export log file ${LOGFILE}"; writetolog
		else
			LOGMSG="Error while exporting Export deffinition! View the export log file ${LOGFILE}"; writetolog
			echo -e "Error while exporting Export deffinition!\n For more information, view the export log file ${LOGFILE}" | mailx -s "${EMAIL_SUBJ}" $EMAILADDR
			exit 1
		fi
	};
	else
	LOGMSG="The export of Export deffinition has finished successfully"; writetolog
fi
#5 export Communication deffinition
LOGMSG="starting exporting exporting Communication deffinition ... "; writetolog
script=ExportCommDef
LOGFILE="$LOG_DIR/${script}_${timestamp}.log"
$PACKAGE_DIR/ExportPackage -profile "${PROFILE}" -package "$EXPORT_DIR/commDef.spk" -objects "/RB Marketing Automation/Business Contexts/Retail Bank - Marketing/Communication Definitions(Folder)" -subprop -log $LOGFILE
rc=$?
if [ $rc -gt 0 ] ; then 
	{
		if [ $rc -eq 4 ]; then 
			LOGMSG="Warning while exporting Communication deffinition! View the export log file ${LOGFILE}"; writetolog
		else
			LOGMSG="Error while exporting Communication deffinition! View the export log file ${LOGFILE}"; writetolog
			echo -e "Error while exporting Communication deffinition!\n For more information, view the export log file ${LOGFILE}" | mailx -s "${EMAIL_SUBJ}" $EMAILADDR
			exit 1
		fi
	};
	else
	LOGMSG="The export of Communication deffinition has finished successfully"; writetolog
fi
LOGMSG="The export process has finished successfully!"; writetolog
