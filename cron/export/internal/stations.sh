cd ~/.meteor

DB_USER=`sed -nr "/^\[database\]/ { :l /^user[ ]*=/ { s/.*=[ ]*//; p; q;}; n; b l;}" ./config.ini`
DB_PASS=`sed -nr "/^\[database\]/ { :l /^password[ ]*=/ { s/.*=[ ]*//; p; q;}; n; b l;}" ./config.ini`
DB_NAME=`sed -nr "/^\[database\]/ { :l /^name[ ]*=/ { s/.*=[ ]*//; p; q;}; n; b l;}" ./config.ini`
FTP_HOST=`sed -nr "/^\[bulk_ftp\]/ { :l /^host[ ]*=/ { s/.*=[ ]*//; p; q;}; n; b l;}" ./config.ini`
FTP_USER=`sed -nr "/^\[bulk_ftp\]/ { :l /^user[ ]*=/ { s/.*=[ ]*//; p; q;}; n; b l;}" ./config.ini`
FTP_PASS=`sed -nr "/^\[bulk_ftp\]/ { :l /^password[ ]*=/ { s/.*=[ ]*//; p; q;}; n; b l;}" ./config.ini`

mysqldump -u$DB_USER -p$DB_PASS $DB_NAME stations > stations.sql
mysqldump -u$DB_USER -p$DB_PASS $DB_NAME inventory > inventory.sql
mysqldump -u$DB_USER -p$DB_PASS $DB_NAME stations_inventory > stations_inventory.sql

curl -T stations.sql ftp://$FTP_HOST/stations/ --user $FTP_USER:$FTP_PASS
curl -T inventory.sql ftp://$FTP_HOST/stations/ --user $FTP_USER:$FTP_PASS
curl -T stations_inventory.sql ftp://$FTP_HOST/stations/ --user $FTP_USER:$FTP_PASS

rm stations.sql
rm inventory.sql
rm stations_inventory.sql
