cd ~/.routines

mysqldump -uroot -p`cat /etc/psa/.psa.shadow` meteostat stations > stations.sql
mysqldump -uroot -p`cat /etc/psa/.psa.shadow` meteostat stations_inventory > stations_inventory.sql

HOST=`sed -nr "/^\[bulk_ftp\]/ { :l /^host[ ]*=/ { s/.*=[ ]*//; p; q;}; n; b l;}" ./config.txt`
USER=`sed -nr "/^\[bulk_ftp\]/ { :l /^user[ ]*=/ { s/.*=[ ]*//; p; q;}; n; b l;}" ./config.txt`
PASS=`sed -nr "/^\[bulk_ftp\]/ { :l /^password[ ]*=/ { s/.*=[ ]*//; p; q;}; n; b l;}" ./config.txt`

ftp -in -u ftp://{$USER}:{$PASS}@{$HOST}/internal/stations.sql stations.sql
ftp -in -u ftp://{$USER}:{$PASS}@{$HOST}/internal/stations.sql stations_inventory.sql

rm stations.sql
rm stations_inventory.sql
