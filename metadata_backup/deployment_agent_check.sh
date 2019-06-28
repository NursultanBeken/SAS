#!/bin/bash
TIME_STAMP=$(date +%s)
export SASHOME=/opt/sas/sashome
DEEPLOY_AGENT_HOME=/opt/sas/sashome/SASDeploymentAgent/9.4
EMAIL=

HOST_NAME=$(hostname)
RUN_STATUS=$(${DEEPLOY_AGENT_HOME}/sas.deployd status | awk '{print $5}' )
rc=$?
if [ $rc -gt 0 ]  
then 
	echo -e "### Failed to call DEPLOYMENT_AGENT!"
    echo -e "Failed to call DEPLOYMENT_AGENT!" | mail -s "$SUBJECT" $EMAIL 
	exit 1
fi

if [ ! -f /saswork/deployAgentflag.txt ] && [ "$RUN_STATUS" != "running." ]; then
	touch /saswork/deployAgentflag.txt
	SUBJECT="SAS Deployment Agent is down on server: ${HOST_NAME}"
	echo -e "The last SAS Deployment Agent status check has given these results:\n\n$RUN_STATUS\n\n Please start the Deployment Agent" | mail -s "$SUBJECT" $EMAIL  
fi

if  [ -f /saswork/deployAgentflag.txt ] && [ "$RUN_STATUS" == "running." ]; then
	SUBJECT="SAS Deployment Agent is runing on server: ${HOST_NAME}"
	echo -e "The SAS Deployment Agent have been restarted." | mail -s "$SUBJECT" $EMAIL 
	rm /saswork/deployAgentflag.txt
fi

echo "### HOSTNAME=$HOST_NAME"
echo "### TIME_STAMP=$TIME_STAMP"
echo "### STATUS=$RUN_STATUS"