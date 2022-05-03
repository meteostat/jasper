cd ~/.jasper

DB_USER=`sed -nr "/^\[db\]/ { :l /^user[ ]*=/ { s/.*=[ ]*//; p; q;}; n; b l;}" ./config.ini`
DB_PASS=`sed -nr "/^\[db\]/ { :l /^password[ ]*=/ { s/.*=[ ]*//; p; q;}; n; b l;}" ./config.ini`
DB_NAME=`sed -nr "/^\[db\]/ { :l /^name[ ]*=/ { s/.*=[ ]*//; p; q;}; n; b l;}" ./config.ini`
FTP_HOST=`sed -nr "/^\[bulk\]/ { :l /^host[ ]*=/ { s/.*=[ ]*//; p; q;}; n; b l;}" ./config.ini`
FTP_USER=`sed -nr "/^\[bulk\]/ { :l /^user[ ]*=/ { s/.*=[ ]*//; p; q;}; n; b l;}" ./config.ini`
FTP_PASS=`sed -nr "/^\[bulk\]/ { :l /^password[ ]*=/ { s/.*=[ ]*//; p; q;}; n; b l;}" ./config.ini`

mysqldump -u$DB_USER -p$DB_PASS $DB_NAME stations > stations.sql
mysqldump -u$DB_USER -p$DB_PASS $DB_NAME inventory > inventory.sql
mysqldump -u$DB_USER -p$DB_PASS $DB_NAME stations_inventory > stations_inventory.sql

curl -T stations.sql ftp://$FTP_HOST/stations/ --user $FTP_USER:$FTP_PASS
curl -T inventory.sql ftp://$FTP_HOST/stations/ --user $FTP_USER:$FTP_PASS
curl -T stations_inventory.sql ftp://$FTP_HOST/stations/ --user $FTP_USER:$FTP_PASS

rm stations.sql
rm inventory.sql
rm stations_inventory.sql
