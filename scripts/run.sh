#! /bin/bash

DIR=/home/btdev/jmiao/projects/conversions
BINDIR=$DIR/bin
CONFDIR=$DIR/conf
LOGDIR=$DIR/log
JARDIR=$DIR/java
HADOOP_CMD=/usr/bin/hadoop

# Generate conf file
PIXEL_CMD=$BINDIR/get_pixels.py
CONFFILE=$CONFDIR/rt.conf
TODAY=$1
NDAYS=7
DATADATE=$(/bin/date -d "1 days ago $TODAY" +"%Y%m%d")
echo "StartDate=$DATADATE" >$CONFFILE
echo "Days=$NDAYS" >>$CONFFILE
# echo "OutputPath=/user/btdev/projects/modeldata/prod/rt_daily/$DATADATE" >>$CONFFILE
echo "OutputPath=/user/btdev/projects/modeldata/prod/rt_daily" >>$CONFFILE
/usr/bin/python $PIXEL_CMD >>$CONFFILE

# Process RT and RTB data
JARFILE=$JARDIR/SocialOpt.jar
JARCLASS="com.sharethis.socialopt.rt.SequenceDataRunner"
LOGFILE=$LOGDIR/rt.$DATADATE.log
LOGCONFFILE=$CONFDIR/log4j.properties
HADOOP_OPTS="-conf /home/btdev/conf/core-prod.xml -Dmapreduce.reduce.memory.mb=8192 -Dmapreduce.reduce.java.opts=-Xmx4096m"
$HADOOP_CMD jar $JARFILE $JARCLASS $HADOOP_OPTS -c $CONFFILE -l $LOGCONFFILE >$LOGFILE

# Process MOAT data
JARCLASS="com.sharethis.socialopt.moat.MoatSequenceRunner"
DATADATE=$(/bin/date -d "2 days ago $TODAY" +"%Y%m%d")
LOGFILE=$LOGDIR/moat.$DATADATE.log
OUTPUT_PATH="/user/btdev/projects/modeldata/prod/moat_daily/$DATADATE"
$HADOOP_CMD jar $JARFILE $JARCLASS $HADOOP_OPTS -d $DATADATE -l $LOGCONFFILE -n 2 -o $OUTPUT_PATH >$LOGFILE

# Save to S3
HADOOP_OPTS="-conf /home/btdev/conf/core-prod.xml -Dmapred.job.queue.name=bt.hourly -Ddistcp.bytes.per.map=102400 -overwrite"
while [ $NDAYS -gt 0 ]
do
	DATADATE=$(/bin/date -d "$NDAYS days ago $TODAY" +"%Y%m%d")
	$HADOOP_CMD distcp $HADOOP_OPTS /user/btdev/projects/modeldata/prod/rt_daily/$DATADATE s3n://sharethis-insights-backup/rt_daily/$DATADATE
	(( NDAYS-- ))
done
DATADATE=$(/bin/date -d "2 days ago $TODAY" +"%Y%m%d")
$HADOOP_CMD distcp $HADOOP_OPTS $OUTPUT_PATH s3n://sharethis-insights-backup/moat_daily/$DATADATE

# Cleaning: delete logs older than 30 days
find $LOGDIR/* -mtime +30 -exec rm {} \;
